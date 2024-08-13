import os
import pandas as pd
from dotenv import load_dotenv
import time
import requests

from services.postgres_connection import PostgresConnection
from services.holiday_api_client import HolidayAPIClient

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def update_locations_table(db_conn):
    truncate_query = "TRUNCATE TABLE locations;"
    db_conn.execute_query(truncate_query)

    insert_query = open("locations_data.sql", 'r').read()
    db_conn.execute_query(insert_query)

def get_latest_holidays(db_conn):
    db_conn.execute_query("select distinct country_code from locations ;")
    country_codes = db_conn.fetch_results()

    country_codes_params = ','.join(tup[0] for tup in country_codes)
    print(country_codes_params)

    url = "https://holidayapi.com/v1/holidays?year=2023&country=NL,GB&pretty&key=8043515a-8e63-4af6-b7e9-18eb32aaed20&subdivisions"

    parameters = {
        'country': country_codes_params,
        'year': 2023,
        'subdivisions': True,
        'pretty': True
    }

    holidays = hapi.get_holidays(parameters)
    print(holidays)
    df = pd.DataFrame(holidays['holidays'])
    print(df)

    holidays_table = df.drop(columns=['subdivisions'])
    holidays_table['weekday_date_name'] = pd.json_normalize(holidays_table['weekday'])['date.name']
    holidays_table['weekday_date_numeric'] = pd.json_normalize(holidays_table['weekday'])['date.numeric']
    holidays_table['weekday_observed_name'] = pd.json_normalize(holidays_table['weekday'])['observed.name']
    holidays_table['weekday_observed_numeric'] = pd.json_normalize(holidays_table['weekday'])['observed.numeric']

    del holidays_table['weekday']
    print(holidays_table)
    db_conn.insert_dataframe(holidays_table, 'holidays')

# Example usage:
if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv('API_KEY')
    database_url = os.getenv('DATABASE_NAME')

    db_conn = PostgresConnection(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD")
    )
    hapi = HolidayAPIClient(os.getenv("API_KEY"))

    db_conn.connect()

    # Example query
    # db_conn.execute_query("SELECT version();")
    # results = db_conn.fetch_results()
    # print(results)

    # update_locations_table(db_conn)
    get_latest_holidays(db_conn)

    # Close the connection
    db_conn.close()
