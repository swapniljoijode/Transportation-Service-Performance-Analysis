from DatabaseConnector import DatabaseConnector
import pandas as pd
def insert_in_table(table_name, df, connector):
    for col in df.columns:
        if df[col].dtypes == 'float':
            df[col] = df[col].fillna(0)

    # Define batch size
    batch_size = 1000  # Adjust this value based on your dataset size and system resources

    # Initialize an empty list to store rows for batch insertion
    rows_to_insert = []

    # Iterate over DataFrame rows
    for row in df.itertuples():
        # Append row to the list
        rows_to_insert.append((
            row.vendorid,
            row.trip_distance,
            row.pickup_datetime,
            row.dropoff_datetime,
            row.pulocationid,
            row.dolocationid,
            row.passenger_count,
            row.ratecodeid,
            row.store_and_fwd_flag,
            row.payment_type,
            row.fare_amount,
            row.mta_tax,
            row.improvement_surcharge,
            row.tip_amount,
            row.tolls_amount,
            row.total_amount,
            row.congestion_surcharge,
            row.airport_fee
        ))

        # Check if batch size is reached
        if len(rows_to_insert) == batch_size:
            # Bulk insert the batch into the database
            connector.insert_data(table_name, f'''
                INSERT INTO {table_name}
                (vendorid, trip_distance, pickup_datetime, dropoff_datetime, pulocationid, dolocationid, passenger_count, ratecodeid, store_and_fwd_flag, payment_type, fare_amount, mta_tax, improvement_surcharge, tip_amount, tolls_amount, total_amount, congestion_surcharge, airport_fee)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows_to_insert)
            connector.commit()

            # Clear the list for the next batch
            rows_to_insert = []

    # Insert any remaining rows
    if rows_to_insert:
        connector.insert_data(table_name, f'''
            INSERT INTO {table_name}
            (vendorid, trip_distance, pickup_datetime, dropoff_datetime, pulocationid, dolocationid, passenger_count, ratecodeid, store_and_fwd_flag, payment_type, fare_amount, mta_tax, improvement_surcharge, tip_amount, tolls_amount, total_amount, congestion_surcharge, airport_fee)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows_to_insert)
        connector.commit()