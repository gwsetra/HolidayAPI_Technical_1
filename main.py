import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request

from services.helper import update_holidays_from_api, update_locations_table, format_date
from services.postgres_connection import PostgresConnection
from services.holiday_api_client import HolidayAPIClient

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

app = Flask(__name__)

@app.route('/refresh_locations_from_sql', methods=['POST'])
def refresh_locations_from_sql():
    """Refresh locations data by reading data from local SQL file, update locations table and return a success message."""
    try:
        response = update_locations_table(db_conn)

        return jsonify({"message": response})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/refresh_holidays_data', methods=['POST'])
def refresh_holidays_data():
    """Refresh holiday data by getting data from the API, update holidays and holidays_subdivisions table and return a success message."""
    try:
        response = update_holidays_from_api(db_conn, holiday_client)

        return jsonify({"message": response})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/find_holidays/<location_id>', methods=['GET'])
def find_holidays(location_id):
    """Retrieve and return holiday data for a specified location and date range."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # read url input parameters
    if not start_date:
        return jsonify({"error": "Missing required parameter: 'start_date'"}), 400
    if not end_date:
        return jsonify({"error": "Missing or invalid required parameter: 'end_date'"}), 400

    try:
        # read template SQL to get holidays by locationid and date range and replace with input parameter values, store to variable
        insert_query = open("find_holidays_by_locationid_and_date_range.sql", 'r').read()
        insert_query = (insert_query.replace('{location_id_params}', location_id)
                        .replace('{date_range_start}', start_date)
                        .replace('{date_range_end}', end_date))
        db_conn.execute_query(insert_query)
        holiday_data = db_conn.fetch_results()

        # Transform the data
        formatted_data = []

        # format the output
        for row in holiday_data:
            formatted_row = {
                "uuid": row[0],
                "location_id": row[6],
                "holiday_name": row[1],
                "holiday_date": format_date(row[2]),
                "holiday_observed": format_date(row[3]),
                "country_code": row[4],
                "subdivision_code": row[5]
            }
            formatted_data.append(formatted_row)

        return jsonify({"holidays": formatted_data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# General Exception Handler
@app.errorhandler(Exception)
def handle_exception(e):
    # Handle generic exceptions
    response = {
        "error": str(e)
    }
    # If the exception is an HTTPException, use its status code
    if hasattr(e, 'code'):
        return jsonify(response), e.code
    # For other exceptions, use a 500 Internal Server Error
    return jsonify(response), 500

# 500 Internal Server Error Handler
@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal Server Error"}), 500

# 404 Not Found Handler
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({"error": "Not Found"}), 404

@app.teardown_appcontext
def teardown(exception):
    """Close the database connection when the application context is torn down."""
    if db_conn is not None:
        db_conn.close()

if __name__ == "__main__":
    # Load environment variable
    load_dotenv()

    # Initiate database connection
    db_conn = PostgresConnection(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD")
    )

    # Initiate HolidayAPI client
    holiday_client = HolidayAPIClient(os.getenv("API_KEY"))

    # Open database connection
    db_conn.connect()

    # Refresh holidays data on first initialisation of service
    update_holidays_from_api(db_conn, holiday_client)
    app.run(debug=True)