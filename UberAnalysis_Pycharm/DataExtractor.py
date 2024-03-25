# DataExtraction.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


class DataExtractor:
    def __init__(self, webpage_url):
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

    def extract(self):
        # Function to perform data extraction
        # Send HTTP GET request to retrieve webpage content
        response = requests.get(self.webpage_url)
        # Parse HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags containing links to Parquet files
        parquet_links = soup.find_all('a', href=lambda href: href.strip().endswith('.parquet'))
        # Directory to save downloaded Parquet files
        output_dir = 'venv/downloaded_parquet_files'
        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Iterate over the Parquet links, filter by year and taxi category, download each file, and load it into a DataFrame
        yellow_dataframes = []
        green_dataframes = []
        for link in parquet_links:
            parquet_url = link['href']
            # Extract the year from the URL
            year = int(parquet_url.split('_')[-1].split('-')[0])
            if year == 2022 and 'green' in parquet_url:
                try:
                    file_path = self.download_parquet_file(parquet_url, output_dir)
                    df = self.read_parquet_to_df(file_path)
                    green_dataframes.append(df)
                except Exception as e:
                    print(f"Error processing Parquet link: {parquet_url}. Error: {e}")
                    continue
            if year == 2022 and 'yellow' in parquet_url:
                try:
                    file_path = self.download_parquet_file(parquet_url, output_dir)
                    df = self.read_parquet_to_df(file_path)
                    yellow_dataframes.append(df)
                except Exception as e:
                    print(f"Error processing Parquet link: {parquet_url}. Error: {e}")
                    continue

        columns_to_include = ['VendorID', 'Trip_distance', 'pickup_datetime', 'dropoff_datetime',
                              'PULocationID', 'DOLocationID', 'RateCodeID', 'Store_and_fwd_flag', 'passenger_count',
                              'Payment_type', 'Fare_amount', 'MTA_tax', 'Improvement_surcharge',
                              'Tip_amount', 'Tolls_amount', 'Total_amount', 'Congestion_Surcharge',
                              'Airport_fee']

        # Lowercase all column names
        columns_to_include_lower = [col.lower() for col in columns_to_include]

        print(columns_to_include_lower)

        for df in yellow_dataframes:
            df.columns = df.columns.str.lower()
            if 'tpep_pickup_datetime' in df.columns:
                df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
            if 'tpep_dropoff_datetime' in df.columns:
                df.rename(columns={'tpep_dropoff_datetime': 'dropoff_datetime'}, inplace=True)
            if 'airport_fee' not in df.columns:
                df['airport_fee'] = 0
            df = df[columns_to_include_lower]

        for df in green_dataframes:
            df.columns = df.columns.str.lower()
            if 'lpep_pickup_datetime' in df.columns:
                df.rename(columns={'lpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
            if 'lpep_dropoff_datetime' in df.columns:
                df.rename(columns={'lpep_dropoff_datetime': 'dropoff_datetime'}, inplace=True)
            if 'airport_fee' not in df.columns:
                df['airport_fee'] = 0
            df = df[columns_to_include_lower]

        return green_dataframes, yellow_dataframes
