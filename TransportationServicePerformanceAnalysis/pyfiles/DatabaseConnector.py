# DatabaseConnection.py

import pyodbc
#import cx_Oracle
#import psycopg2

class DatabaseConnector:
    def __init__(self, db_type, **kwargs):
        # Initializes the DatabaseConnector class with the database type and connection details
        self.db_type = db_type
        self.conn = None
        self.cursor = None
        self.connection_details = kwargs

    def connect(self):
        # Establishes a connection to the database based on the provided database type
        try:
            if self.db_type == 'mssql':
                # Connect to MS SQL Server
                conn_str = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.connection_details['server']};DATABASE={self.connection_details['database']};UID={self.connection_details['username']};PWD={self.connection_details['password']}"
                self.conn = pyodbc.connect(conn_str)
                self.cursor = self.conn.cursor()
                print("Connected to MS SQL Server successfully.")
            elif self.db_type == 'oracle':
                # Connect to Oracle
                conn_str = f"{self.connection_details['username']}/{self.connection_details['password']}@{self.connection_details['dsn']}"
                self.conn = cx_Oracle.connect(conn_str)
                self.cursor = self.conn.cursor()
                print("Connected to Oracle successfully.")
            elif self.db_type == 'postgres':
                # Connect to PostgreSQL
                conn_str = f"dbname={self.connection_details['database']} user={self.connection_details['username']} password={self.connection_details['password']} host={self.connection_details['host']} port={self.connection_details['port']}"
                self.conn = psycopg2.connect(conn_str)
                self.cursor = self.conn.cursor()
                print("Connected to PostgreSQL successfully.")
            else:
                print("Invalid database type.")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        # Closes the database connection
        try:
            if self.conn:
                self.conn.close()
                print("Connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")

    def execute_query(self, query):
        # Executes a query on the database
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print("Query executed successfully.")
        except Exception as e:
            print(f"Error executing query: {e}")

    def commit(self):
        # Commits a transaction
        try:
            self.conn.commit()
            print("Transaction committed successfully.")
        except Exception as e:
            print(f"Error committing transaction: {e}")

    def create_table(self, table_name, create_table_query):
        # Creates a new table in the database
        try:
            # Check if table exists, if not, create it
            self.execute_query(
                f"IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}') BEGIN {create_table_query} END")
            print(f"Table '{table_name}' has been created successfully.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")

    def insert_data(self, table_name, insert_query, data):
        # Inserts data into a table in the database
        try:
            self.cursor.executemany(insert_query, data)
            print("Data insertion completed successfully.")
        except Exception as e:
            print(f"Error inserting data into '{table_name}': {e}")
