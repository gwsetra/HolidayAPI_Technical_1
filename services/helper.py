import pandas as pd
import numpy as np
def format_date(date_input):
    try:
        return date_input.strftime('%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        return date_input  # Return the original string if parsing fails

def update_locations_table(db_conn):
    """Overwrite locations table with data executed from locations_data.sql file"""
    try:
        truncate_query = "TRUNCATE TABLE locations;"
        db_conn.execute_query(truncate_query)

        insert_query = open("locations_data.sql", 'r').read()
        db_conn.execute_query(insert_query)
    except Exception as error:
        raise Exception('Issue connecting with Database')

    return 'locations table updated successfully'

def get_country_codes_from_db(db_conn):
    """get distinct country codes from locations table"""
    db_conn.execute_query("select distinct country_code from locations;")
    country_codes = db_conn.fetch_results()

    return country_codes

def update_holidays_from_api(db_conn, holiday_client):
    """
    Fetch holiday data from an API and update the database.

    Args:
        db_conn (DatabaseConnection): Connection object for database operations.
        holiday_client (HolidayClient): Client object for accessing the holiday API.

    Returns:
        String: Confirmation of process state
    """
    # Get country codes from the database
    try:
        country_codes_tuple = get_country_codes_from_db(db_conn)
        country_codes_params = ','.join(tup[0] for tup in country_codes_tuple)
    except Exception as error:
        raise Exception('Issue connecting with Database')

    # Define API parameters
    parameters = {
        'country': country_codes_params,
        'year': 2023,
        'subdivisions': True,
        'pretty': True
    }

    # Fetch and create holiday dataframe

    holidays = holiday_client.get_holidays(parameters)
    df = pd.DataFrame(holidays['holidays'])

    try:
        holidays_table = df.drop(columns=['subdivisions'])
        holidays_table['weekday_date_name'] = pd.json_normalize(holidays_table['weekday'])['date.name']
        holidays_table['weekday_date_numeric'] = pd.json_normalize(holidays_table['weekday'])['date.numeric']
        holidays_table['weekday_observed_name'] = pd.json_normalize(holidays_table['weekday'])['observed.name']
        holidays_table['weekday_observed_numeric'] = pd.json_normalize(holidays_table['weekday'])['observed.numeric']

        del holidays_table['weekday']

        # Update holidays table in the database
        truncate_query = "TRUNCATE TABLE holidays;"
        db_conn.execute_query(truncate_query)

        # insert holidays information to holidays table
        db_conn.insert_dataframe(holidays_table, 'holidays')

        # create holiday_subdivisions dataframe
        holidays_subdivisions = df[['uuid', 'subdivisions']].explode('subdivisions')
        holidays_subdivisions['subdivisions'] = holidays_subdivisions['subdivisions'].replace({np.nan: None})

        # Update holidays_subdivisions table
        truncate_query = "TRUNCATE TABLE holidays_subdivisions;"
        db_conn.execute_query(truncate_query)
        db_conn.insert_dataframe(holidays_subdivisions, 'holidays_subdivisions')
    except Exception as error:
        raise Exception('Internal Server Error')

    return 'holidays and holidays_subdivisions table updated successfully'

def get_countries_from_api(db_conn, holiday_client):
    """
    Fetch country data from an API and update the `country_subdivisions` table in the database.

    Args:
        db_conn (DatabaseConnection): Connection object for database operations.
        holiday_client (HolidayClient): Client object for accessing the holiday API.

    Returns:
        String: Confirmation of process state
    """
    # Retrieve country codes from the database
    country_codes_tuple = get_country_codes_from_db(db_conn)
    list_of_countries = [t[0] for t in country_codes_tuple]

    # Define API parameters
    parameters = {
        'year': 2023,
        'pretty': True
    }

    # Fetch country data from the API
    countries = holiday_client.get_countries(parameters)
    latest_countries_list = []

    # Process and transform the country data into a DataFrame
    for country in countries['countries']:
        match_found = country['code'] in list_of_countries
        if match_found:
            latest_countries_list.append(country)
    countries_df = pd.DataFrame(latest_countries_list)

    tmp_countries_df = countries_df[['code', 'name', 'subdivisions']].explode('subdivisions')
    code_series = tmp_countries_df['subdivisions'].apply(lambda x: x['code']).rename('subdivisions_code')
    name_series = tmp_countries_df['subdivisions'].apply(lambda x: x['name']).rename('subdivisions_name')

    combined_countries_df = pd.concat([tmp_countries_df[['code', 'name']], code_series, name_series], axis=1)

    # Update the database
    truncate_query = "TRUNCATE TABLE country_subdivisions;"
    db_conn.execute_query(truncate_query)
    db_conn.insert_dataframe(combined_countries_df, 'country_subdivisions')

    return 'country_divisions table updated successfully'