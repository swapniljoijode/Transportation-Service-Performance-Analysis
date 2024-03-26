from DatabaseConnector import DatabaseConnector
import pandas as pd

def create_table(df, year, month, connector):
    # Function to create a table in the database for storing trip data of a specific car type for a given year and month

    # Specify the table name based on car type, year, and month
    table_name = f'trips_{year}_{month}'

    # Construct the create table query
    create_table_query = f"""
    CREATE TABLE {table_name} (
        tripid INT IDENTITY(1,1) PRIMARY KEY,
        vendorid NVARCHAR(MAX),
        trip_distance FLOAT,
        pickup_datetime DATETIME,
        dropoff_datetime DATETIME,
        pulocationid INT,
        dolocationid INT,
        passenger_count FLOAT,
        ratecodeid FLOAT,
        store_and_fwd_flag NVARCHAR(2),
        payment_type NVARCHAR(50),
        fare_amount FLOAT,
        mta_tax FLOAT,
        improvement_surcharge FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT,
        car_type INT
    )
    """

    # Create the table in the database using the connector
    connector.create_table(table_name, create_table_query)

    return table_name
