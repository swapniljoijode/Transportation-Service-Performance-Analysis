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
green_dataframes, yellow_dataframes, required_year = extractor.extract()

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

monthly_combined_data = pd.DataFrame()
month_list = [df['pickup_datetime'].dt.month.iloc[0] for df in yellow_dataframes]
# Process the extracted data and insert into the database
for month in month_list:
    for df in yellow_dataframes:
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['car_type'] = 1
        # Extract month and year
        pickup_month = (df['pickup_datetime'].dt.month.iloc[0])
        pickup_year = (df['pickup_datetime'].dt.year.iloc[0])


        if pickup_year == required_year and pickup_month == month:
            df_year = df[(df['pickup_datetime'].dt.year == pickup_year)] #filter the dates by year
            df_year_month = df_year[(df_year['pickup_datetime'].dt.month == pickup_month)] #filter the dates by month
            grouped = df.groupby(df_year_month['pickup_datetime'].dt.date)

            # Initialize an empty list to store sampled dataframes
            sampled_dfs = []
            yellow_monthly_combined_data = pd.DataFrame()
            # Sample 1000 rows at random for each group
            for _, group_df in grouped:
                if len(group_df) >= 1000:
                    sampled_df = group_df.sample(n=1000, random_state=42)  # Set random_state for reproducibility
                else:
                    sampled_df = group_df  # Keep all rows if less than 1000
                sampled_dfs.append(sampled_df)

            for new_df in sampled_dfs:
                yellow_monthly_combined_data = pd.concat([yellow_monthly_combined_data, new_df], ignore_index=True)
            monthly_combined_data = pd.concat([monthly_combined_data, yellow_monthly_combined_data], ignore_index=True)

        else:
            continue

    # Process green car data
    for df in green_dataframes:
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        # add new column to the dataframe for car type. 1: yellow cars. 2: green cars
        df['car_type'] = 2

        # Extract month and year
        pickup_month = (df['pickup_datetime'].dt.month.iloc[0])
        pickup_year = (df['pickup_datetime'].dt.year.iloc[0])


        if pickup_year == required_year and pickup_month == month:
            df_year = df[(df['pickup_datetime'].dt.year == pickup_year)] #filter the dates by year
            df_year_month = df_year[(df_year['pickup_datetime'].dt.month == pickup_month)] #filter the dates by month
            grouped = df.groupby(df_year_month['pickup_datetime'].dt.date)

            # Initialize an empty list to store sampled dataframes
            sampled_dfs = []
            green_monthly_combined_data = pd.DataFrame()

            # Sample 1000 rows at random for each group
            for _, group_df in grouped:
                if len(group_df) >= 1000:
                    sampled_df = group_df.sample(n=1000, random_state=42)  # Set random_state for reproducibility
                else:
                    sampled_df = group_df  # Keep all rows if less than 1000
                sampled_dfs.append(sampled_df)

            for new_df in sampled_dfs:
                green_monthly_combined_data = pd.concat([green_monthly_combined_data, new_df], ignore_index=True)
            monthly_combined_data = pd.concat([monthly_combined_data, green_monthly_combined_data], ignore_index=True)


        else:
            continue

    table_name = TableCreation.create_table(monthly_combined_data,required_year,month,mssql_connector)
    TableInsertion.insert_in_table(table_name,monthly_combined_data,mssql_connector)
    monthly_combined_data = pd.DataFrame()

mssql_connector.close()