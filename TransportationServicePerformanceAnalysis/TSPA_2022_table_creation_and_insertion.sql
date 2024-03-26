USE [UberAnalytics]
GO

-- Check the data for the 1st month of 2022
SELECT [tripid]
      ,[vendorid]
      ,[trip_distance]
      ,[pickup_datetime]
      ,[dropoff_datetime]
      ,[pulocationid]
      ,[dolocationid]
      ,[passenger_count]
      ,[ratecodeid]
      ,[store_and_fwd_flag]
      ,[payment_type]
      ,[fare_amount]
      ,[mta_tax]
      ,[improvement_surcharge]
      ,[tip_amount]
      ,[tolls_amount]
      ,[total_amount]
      ,[congestion_surcharge]
      ,[airport_fee]
      ,[car_type]
FROM [dbo].[trips_2022_1]
;

--check if all the month's data can be combined together and viewed as a single table.
SELECT [vendorid]
      ,[trip_distance]
      ,[pickup_datetime]
      ,[dropoff_datetime]
      ,[pulocationid]
      ,[dolocationid]
      ,[passenger_count]
      ,[ratecodeid]
      ,[store_and_fwd_flag]
      ,[payment_type]
      ,[fare_amount]
      ,[mta_tax]
      ,[improvement_surcharge]
      ,[tip_amount]
      ,[tolls_amount]
      ,[total_amount]
      ,[congestion_surcharge]
      ,[airport_fee]
      ,[car_type]
from (SELECT * 
		FROM (
		SELECT * FROM [dbo].[trips_2022_1]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_2]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_3]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_4]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_5]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_6]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_7]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_8]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_9]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_10]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_11]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_12]
		-- Add more SELECT statements for additional tables
		) AS all_tables
	) as merged_tables
;	

--create a new table to store the combined data of all the months
create table trip_data_2022 (
	[tripid] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	[vendorid] [nvarchar](max) NULL,
	[trip_distance] [float] NULL,
	[pickup_datetime] [datetime] NULL,
	[dropoff_datetime] [datetime] NULL,
	[pulocationid] [int] NULL,
	[dolocationid] [int] NULL,
	[passenger_count] [float] NULL,
	[ratecodeid] [float] NULL,
	[store_and_fwd_flag] [nvarchar](2) NULL,
	[payment_type] [nvarchar](50) NULL,
	[fare_amount] [float] NULL,
	[mta_tax] [float] NULL,
	[improvement_surcharge] [float] NULL,
	[tip_amount] [float] NULL,
	[tolls_amount] [float] NULL,
	[total_amount] [float] NULL,
	[congestion_surcharge] [float] NULL,
	[airport_fee] [float] NULL,
	[car_type] [int] NULL
);

--insert data in the universal table from month tables.
insert into trip_data_2022
SELECT [vendorid]
      ,[trip_distance]
      ,[pickup_datetime]
      ,[dropoff_datetime]
      ,[pulocationid]
      ,[dolocationid]
      ,[passenger_count]
      ,[ratecodeid]
      ,[store_and_fwd_flag]
      ,[payment_type]
      ,[fare_amount]
      ,[mta_tax]
      ,[improvement_surcharge]
      ,[tip_amount]
      ,[tolls_amount]
      ,[total_amount]
      ,[congestion_surcharge]
      ,[airport_fee]
      ,[car_type]
from (SELECT * 
		FROM (
		SELECT * FROM [dbo].[trips_2022_1]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_2]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_3]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_4]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_5]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_6]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_7]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_8]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_9]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_10]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_11]
		UNION ALL
		SELECT * FROM [dbo].[trips_2022_12]
		-- Add more SELECT statements for additional tables
		) AS all_tables
) as merged_tables
;