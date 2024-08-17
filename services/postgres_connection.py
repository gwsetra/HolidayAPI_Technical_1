import pandas as pd
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import execute_values

class PostgresConnection:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establishes a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
        except OperationalError as e:
            print(f"Failed to connect to PostgreSQL database: {e}")
            self.connection = None
            raise Exception(f"Failed to connect to PostgreSQL database: {e}")

    def execute_query(self, query, params=None):
        """Executes a SQL query."""
        if self.connection is None:
            print("No connection to the database.")
            return None

        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()

    def fetch_results(self):
        """Fetches the results from the last executed query."""
        if self.cursor:
            return self.cursor.fetchall()
        return None

    def insert_dataframe(self, df, table_name):
        """Inserts data from a pandas DataFrame into the specified PostgreSQL table."""
        if self.connection is None:
            print("No connection to the database.")
            return

        if df.empty:
            print("DataFrame is empty.")
            return

        # Prepare the SQL query
        cols = ', '.join(df.columns)
        values_placeholder = ', '.join(['%s'] * len(df.columns))
        insert_query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES %s"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(cols)
        )

        # Convert DataFrame to list of tuples
        data_tuples = list(df.itertuples(index=False, name=None))

        try:
            # Use execute_values for efficient bulk insert
            execute_values(self.cursor, insert_query, data_tuples)
            self.connection.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.connection.rollback()

    def close(self):
        """Closes the cursor and connection to the PostgreSQL database."""
        if self.cursor:
            self.cursor.close()
            print("Cursor closed.")
        if self.connection:
            self.connection.close()
            print("Connection closed.")