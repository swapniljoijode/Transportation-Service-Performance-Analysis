from DataExtractor import DataExtractor
from DatabaseConnector import DatabaseConnector
import pandas as pd
import TableCreation
import TableInsertion

'''Extracting Datafiles from the URL'''
# URL of the webpage containing the Parquet file URLs
webpage_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

# Create an instance of DataExtractor
extractor = DataExtractor(webpage_url)

# Extract data using DataExtractor.extract() method
green_dataframes, yellow_dataframes = extractor.extract()

'''Connecting to the server'''
# Requesting user for Connection details for MS SQL Server
print('*Please provide the server details for database connection*')
print('____________________________________________________________')
db_type = input('Enter the type of database: ').lower()
server_name = input('Enter the name of the server: ')
database_name = input('Enter the name of the database: ')
user_name = input('Enter the name of the user: ')
password = input('Enter the password for the user: ')

# Store server details in a dictionary
server_details = {
    'server': server_name,
    'database': database_name,
    'username': user_name,
    'password': password
}

# Create a DatabaseConnector instance for MS SQL Server
mssql_connector = DatabaseConnector(db_type=db_type, **server_details)

# Connect to the database
mssql_connector.connect()

print('Please provide the details of the data required\n')
'''Ask the user for the year and month of the required data'''
required_year = int(input('Enter the year of the required data: '))

# Process the extracted data and insert into the database
for df in yellow_dataframes:
    car_type = 'yellow_cars'
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    # Extract month and year
    pickup_month = (df['pickup_datetime'].dt.month.iloc[0])
    pickup_year = (df['pickup_datetime'].dt.year.iloc[0])

    required_data = pd.DataFrame()
    if pickup_year == required_year:
        df_year = df[(df['pickup_datetime'].dt.year == pickup_year)] #filter the dates by year
        df_year_month = df_year[(df_year['pickup_datetime'].dt.month == pickup_month)] #filter the dates by month
        grouped = df.groupby(df_year_month['pickup_datetime'].dt.date)

        # Initialize an empty list to store sampled dataframes
        sampled_dfs = []

        # Sample 1000 rows at random for each group
        for _, group_df in grouped:
            if len(group_df) >= 1000:
                sampled_df = group_df.sample(n=1000, random_state=42)  # Set random_state for reproducibility
            else:
                sampled_df = group_df  # Keep all rows if less than 1000
            sampled_dfs.append(sampled_df)

        for new_df in sampled_dfs:
            required_data = pd.concat([required_data, new_df], ignore_index=True)

        table_name = TableCreation.create_table(car_type, required_data, pickup_year, pickup_month, mssql_connector)
        TableInsertion.insert_in_table(table_name, required_data, mssql_connector)

    else:
        continue

# Process green car data
for df in green_dataframes:
    car_type = 'green_cars'
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

    # Extract month and year
    pickup_month = (df['pickup_datetime'].dt.month.iloc[0])
    pickup_year = (df['pickup_datetime'].dt.year.iloc[0])

    required_data = pd.DataFrame()
    if pickup_year == required_year:
        df_year = df[(df['pickup_datetime'].dt.year == pickup_year)] #filter the dates by year
        df_year_month = df_year[(df_year['pickup_datetime'].dt.month == pickup_month)] #filter the dates by month
        grouped = df.groupby(df_year_month['pickup_datetime'].dt.date)

        # Initialize an empty list to store sampled dataframes
        sampled_dfs = []

        # Sample 1000 rows at random for each group
        for _, group_df in grouped:
            if len(group_df) >= 1000:
                sampled_df = group_df.sample(n=1000, random_state=42)  # Set random_state for reproducibility
            else:
                sampled_df = group_df  # Keep all rows if less than 1000
            sampled_dfs.append(sampled_df)

        for new_df in sampled_dfs:
            required_data = pd.concat([required_data, new_df], ignore_index=True)

        table_name = TableCreation.create_table(car_type, required_data, pickup_year, pickup_month, mssql_connector)
        TableInsertion.insert_in_table(table_name, required_data, mssql_connector)

    else:
        continue
