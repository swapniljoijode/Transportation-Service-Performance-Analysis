from DatabaseConnector import DatabaseConnector
import pandas as pd

def create_table(car_type,df, year, month, connector):
    # Specify the table name
    table_name = f'{car_type}_trips_{year}_{month}'

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
        airport_fee FLOAT
    )
    """

    # Create the table
    connector.create_table(table_name, create_table_query)

    return table_name