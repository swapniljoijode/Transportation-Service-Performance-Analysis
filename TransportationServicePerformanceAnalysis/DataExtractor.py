# DataExtractor.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

class DataExtractor:
    def __init__(self, webpage_url):
        # Initializes the DataExtractor class with a webpage URL
        self.webpage_url = webpage_url

    def download_parquet_file(self, url, output_dir):
        # Function to download Parquet files from URLs
        # Extract filename from URL
        filename = url.split('/')[-1]
        # Construct output path
        output_path = os.path.join(output_dir, filename)
        # Send HTTP GET request to download the file
        response = requests.get(url)
        # Save the file to the output directory
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")
        return output_path

    def read_parquet_to_df(self, file_path):
        # Function to read Parquet file into DataFrame
        df = pd.read_parquet(file_path)
        return df

    def read_csv_to_df(self,file_path):
        df = pd.read_csv(file_path)
        return df

    def extract(self):
        # Function to perform data extraction
        # Send HTTP GET request to retrieve webpage content
        response = requests.get(self.webpage_url)
        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags containing links to Parquet files
        parquet_links = soup.find_all('a', href=lambda href: href.strip().endswith('.parquet'))
        # Find all <a> tags containing links to csv files
        csv_links = soup.find_all('a', href=lambda href: href.strip().endswith('.csv'))
        # Directory to save downloaded Parquet files
        output_dir = 'venv/downloaded_parquet_files'
        # Directory to save downloaded CSV files
        output_dir_csv = 'venv/downloaded_csv_files'
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(output_dir_csv, exist_ok=True)

        #Download the Location.csv file and load it in the Dataframe
        Location_df = pd.DataFrame()
        for link in csv_links:
            csv_url = link['href']
            file_path = self.download_parquet_file(csv_url, output_dir_csv)
            Location_df = self.read_csv_to_df(file_path)
            for col in Location_df.columns:
                if not col == "LocationID":
                    Location_df[col] = Location_df[col].fillna('Unknown')


        # Initialize lists to store DataFrames for green and yellow taxis
        yellow_dataframes = []
        green_dataframes = []
        # Ask the user to specify which year's data is required
        required_year = int(input('Enter the year of the required data: '))
        # Iterate over the Parquet links, filter by year and taxi category, download each file, and load it into a DataFrame
        for link in parquet_links:
            parquet_url = link['href']
            # Extract the year from the URL
            year = int(parquet_url.split('_')[-1].split('-')[0])
            # Check if the Parquet file is for the year 2022 and belongs to green or yellow taxi category
            if year == required_year and 'green' in parquet_url:
                try:
                    # Download and read the Parquet file into a DataFrame for green taxis
                    file_path = self.download_parquet_file(parquet_url, output_dir)
                    df = self.read_parquet_to_df(file_path)
                    green_dataframes.append(df)
                except Exception as e:
                    print(f"Error processing Parquet link: {parquet_url}. Error: {e}")
                    continue
            if year == required_year and 'yellow' in parquet_url:
                try:
                    # Download and read the Parquet file into a DataFrame for yellow taxis
                    file_path = self.download_parquet_file(parquet_url, output_dir)
                    df = self.read_parquet_to_df(file_path)
                    yellow_dataframes.append(df)
                except Exception as e:
                    print(f"Error processing Parquet link: {parquet_url}. Error: {e}")
                    continue



        yellow_dataframes_transformed = []
        green_dataframes_transformed = []

        vendor = {1: "Creative Mobile Technologies",
                  2: "VeriFone Inc"}

        rate_code_type = {1: "Standard rate",
                          2: "JFK",
                          3: "Newark",
                          4: "Nassau",
                          5: "Negotiated Fare",
                          6: "Group Ride"}

        payment_type = {
            1: "Credit Card",
            2: "Cash",
            3: "No Charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided Trip"
        }

        # List of columns to include in the final DataFrame
        columns_to_include = ['VendorName', 'Trip_distance', 'pickup_datetime', 'dropoff_datetime',
                              'PickupLocation','DropoffLocation', 'RateCode', 'Store_and_fwd_flag', 'passenger_count',
                              'Payment_type', 'Fare_amount', 'MTA_tax', 'Improvement_surcharge',
                              'Tip_amount', 'Tolls_amount', 'Total_amount', 'Congestion_Surcharge', 'Airport_Fee', 'Car_Type'
                              ]

        # Lowercase all column names
        columns_to_include_lower = [col.lower() for col in columns_to_include]

        # Process yellow taxi DataFrames
        for df in yellow_dataframes:
            # Lowercase column names
            # Lowercase column names
            df.columns = df.columns.str.lower()
            # Rename datetime columns if necessary
            if 'tpep_pickup_datetime' in df.columns:
                df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
            if 'tpep_dropoff_datetime' in df.columns:
                df.rename(columns={'tpep_dropoff_datetime': 'dropoff_datetime'}, inplace=True)
            # Add airport_fee column if not present
            if 'airport_fee' not in df.columns:
                df['airport_fee'] = 0
                df['airport_fee'].astype('float')
            df['car_type'] = 'Yellow Cars'
            df['vendorid'] = df['vendorid'].map(vendor)
            df['ratecodeid'] = df['ratecodeid'].map(rate_code_type)
            df['payment_type'] = df['payment_type'].map(payment_type)
            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
            df.rename(columns={'vendorid': 'vendorname', 'ratecodeid': 'ratecode'}, inplace=True)
            df = pd.merge(df, Location_df, left_on='pulocationid', right_on='LocationID', how='left')
            df['pickuplocation'] = df['Borough']+', '+df['Zone']+', '+df['service_zone']
            df.drop(columns=['LocationID','Borough','Zone','service_zone'], inplace=True)
            df = pd.merge(df, Location_df, left_on='dolocationid', right_on='LocationID', how='left')
            df['dropofflocation'] = df['Borough']+', '+df['Zone']+', '+df['service_zone']
            df.drop(columns=['LocationID', 'Borough', 'Zone', 'service_zone'], inplace=True)
            for col in df.columns:
                if df[col].dtype == 'float' or df[col].dtype == 'int':
                    df[col] = df[col].fillna(0)
                elif col == 'vendorname':
                    df[col] = df[col].fillna('Creative Mobile Technologies')
                elif col == 'ratecode':
                    df[col] = df[col].fillna('Standard rate')
                elif col == 'payment_type':
                    df[col] = df[col].fillna('Cash')
                elif col == 'store_and_fwd_flag':
                    df[col] = df[col].fillna('N')
            columns_to_drop = [col for col in df.columns if col not in columns_to_include_lower]
            df.drop(columns=columns_to_drop, inplace=True)
            yellow_dataframes_transformed.append(df)



        # Process green taxi DataFrames
        for df in green_dataframes:
            # Lowercase column names
            df.columns = df.columns.str.lower()
            # Rename datetime columns if necessary
            if 'lpep_pickup_datetime' in df.columns:
                df.rename(columns={'lpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
            if 'lpep_dropoff_datetime' in df.columns:
                df.rename(columns={'lpep_dropoff_datetime': 'dropoff_datetime'}, inplace=True)
            # Add airport_fee column if not present
            if 'airport_fee' not in df.columns:
                df['airport_fee'] = 0
                df['airport_fee'].astype('float')
            df['car_type'] = 'Green Cars'
            df['vendorid'] = df['vendorid'].map(vendor)
            df['ratecodeid'] = df['ratecodeid'].map(rate_code_type)
            df['payment_type'] = df['payment_type'].map(payment_type)
            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
            df.rename(columns={'vendorid': 'vendorname', 'ratecodeid': 'ratecode'}, inplace=True)
            df = pd.merge(df, Location_df, left_on='pulocationid', right_on='LocationID', how='left')
            df['pickuplocation'] = df['Borough'] + ', ' + df['Zone'] + ', ' + df['service_zone']
            df.drop(columns=['LocationID', 'Borough', 'Zone', 'service_zone'], inplace=True)
            df = pd.merge(df, Location_df, left_on='dolocationid', right_on='LocationID', how='left')
            df['dropofflocation'] = df['Borough'] + ', ' + df['Zone'] + ', ' + df['service_zone']
            for col in df.columns:
                if df[col].dtype == 'float' or df[col].dtype == 'int':
                    df[col] = df[col].fillna(0)
                elif col == 'vendorname':
                    df[col] = df[col].fillna('Creative Mobile Technologies')
                elif col == 'ratecode':
                    df[col] = df[col].fillna('Standard rate')
                elif col == 'payment_type':
                    df[col] = df[col].fillna('Cash')
                elif col == 'store_and_fwd_flag':
                    df[col] = df[col].fillna('N')
            # Filter DataFrame columns
            columns_to_drop = [col for col in df.columns if col not in columns_to_include_lower]
            df.drop(columns=columns_to_drop, inplace=True)
            green_dataframes_transformed.append(df)

        # Return processed DataFrames for green and yellow taxis
        return green_dataframes_transformed, yellow_dataframes_transformed, required_year
