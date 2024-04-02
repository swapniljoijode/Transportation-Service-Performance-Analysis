from DatabaseConnector import DatabaseConnector


def create_table(year, month, connector):
    # Function to create a table in the database for storing trip data of a specific car type for a given year and month

    # Specify the table name based on car type, year, and month
    table_name = f'trips_{year}_{month}'

    # Construct the create table query
    create_table_query = f"""
    CREATE TABLE {table_name} (
        tripid INT IDENTITY(1,1) PRIMARY KEY,
        vendorname varchar(50),
        trip_distance FLOAT,
        pickup_datetime DATETIME,
        dropoff_datetime DATETIME,
        pu_borough varchar(50),
        pu_zone varchar(50),
        pu_service_zone varchar(50),
        do_borough varchar(50),
        do_zone varchar(50),
        do_service_zone varchar(50),
        passenger_count FLOAT,
        store_and_fwd_flag VARCHAR(2),
        ratecode varchar(20),
        payment_type VARCHAR(50),
        fare_amount FLOAT,  -- Adjust precision to 18 total digits with 2 decimal places
        mta_tax FLOAT,      -- Adjust precision to 18 total digits with 2 decimal places
        improvement_surcharge FLOAT,  -- Adjust precision to 18 total digits with 2 decimal places
        tip_amount FLOAT,   -- Adjust precision to 18 total digits with 2 decimal places
        tolls_amount FLOAT, -- Adjust precision to 18 total digits with 2 decimal places
        total_amount FLOAT, -- Adjust precision to 18 total digits with 2 decimal places
        congestion_surcharge FLOAT,  -- Adjust precision to 18 total digits with 2 decimal places
        airport_fee FLOAT,  -- Adjust precision to 18 total digits with 2 decimal places
        car_type VARCHAR(50)
    )
    """

    # Create the table in the database using the connector
    connector.create_table(table_name, create_table_query)

    return table_name
