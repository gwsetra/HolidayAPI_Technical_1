import pandas as pd
import numpy as np
def format_date(date_input):
    try:
        return date_input.strftime('%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        return date_input  # Return the original string if parsing fails

def update_locations_table(db_conn):
    truncate_query = "TRUNCATE TABLE locations;"
    db_conn.execute_query(truncate_query)

    insert_query = open("locations_data.sql", 'r').read()
    db_conn.execute_query(insert_query)

def get_country_codes_from_db(db_conn):
    db_conn.execute_query("select distinct country_code from locations;")
    country_codes = db_conn.fetch_results()

    return country_codes

def get_holidays_from_api(db_conn, holiday_client):
    country_codes_tuple = get_country_codes_from_db(db_conn)
    country_codes_params = ','.join(tup[0] for tup in country_codes_tuple)

    parameters = {
        'country': country_codes_params,
        'year': 2023,
        'subdivisions': True,
        'pretty': True
    }

    holidays = holiday_client.get_holidays(parameters)
    df = pd.DataFrame(holidays['holidays'])

    holidays_table = df.drop(columns=['subdivisions'])
    holidays_table['weekday_date_name'] = pd.json_normalize(holidays_table['weekday'])['date.name']
    holidays_table['weekday_date_numeric'] = pd.json_normalize(holidays_table['weekday'])['date.numeric']
    holidays_table['weekday_observed_name'] = pd.json_normalize(holidays_table['weekday'])['observed.name']
    holidays_table['weekday_observed_numeric'] = pd.json_normalize(holidays_table['weekday'])['observed.numeric']

    del holidays_table['weekday']

    truncate_query = "TRUNCATE TABLE holidays;"
    db_conn.execute_query(truncate_query)

    db_conn.insert_dataframe(holidays_table, 'holidays')

    holidays_subdivisions = df[['uuid', 'subdivisions']].explode('subdivisions')
    holidays_subdivisions['subdivisions'] = holidays_subdivisions['subdivisions'].replace({np.nan: None})

    truncate_query = "TRUNCATE TABLE holidays_subdivisions;"
    db_conn.execute_query(truncate_query)
    db_conn.insert_dataframe(holidays_subdivisions, 'holidays_subdivisions')

def get_countries_from_api(db_conn, holiday_client):
    country_codes_tuple = get_country_codes_from_db(db_conn)
    list_of_countries = list_of_strings = [t[0] for t in country_codes_tuple]

    parameters = {
        'year': 2023,
        'pretty': True
    }

    countries = holiday_client.get_countries(parameters)
    latest_countries_list = []

    for country in countries['countries']:
        match_found = country['code'] in list_of_countries
        if match_found:
            latest_countries_list.append(country)
    countries_df = pd.DataFrame(latest_countries_list)

    tmp_countries_df = countries_df[['code', 'name', 'subdivisions']].explode('subdivisions')
    code_series = tmp_countries_df['subdivisions'].apply(lambda x: x['code']).rename('subdivisions_code')
    name_series = tmp_countries_df['subdivisions'].apply(lambda x: x['name']).rename('subdivisions_name')

    combined_countries_df = pd.concat([tmp_countries_df[['code', 'name']], code_series, name_series], axis=1)

    truncate_query = "TRUNCATE TABLE country_subdivisions;"
    db_conn.execute_query(truncate_query)
    db_conn.insert_dataframe(combined_countries_df, 'country_subdivisions')