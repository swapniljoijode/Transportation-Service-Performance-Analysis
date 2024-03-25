🚗 Transportation Service Performance Analysis 📊

Phase 1: Data Extraction, Exploration, Cleaning, and Processing
This project facilitates the extraction, exploration, cleaning, and processing of transportation service data, enabling comprehensive analysis and performance evaluation.

Project Structure 🛠️
1. Data Extraction (DataExtractor.py):
The DataExtractor module is responsible for fetching transportation service data files from specified sources, typically URLs or APIs. It utilizes the requests library for HTTP interactions and BeautifulSoup for web scraping. This module encapsulates the logic for downloading data files and extracting relevant information, such as trip details and timestamps.

2. Database Connection (DatabaseConnector.py):
The DatabaseConnector module provides a flexible interface for connecting to various relational databases. It supports popular DBMSs such as Microsoft SQL Server, Oracle, and PostgreSQL. This module abstracts the database connection process and offers methods for executing SQL queries, creating tables, and inserting data into the database.

3. Table Creation (TableCreation.py):
The TableCreation module defines functions for generating database tables optimized for storing transportation service data. It constructs SQL queries to create tables with appropriate schemas, including columns for trip attributes such as trip ID, pickup/dropoff timestamps, distance, fare details, and geographic locations. This module ensures that the database schema aligns with the structure of the data being stored.

4. Table Insertion (TableInsertion.py):
The TableInsertion module handles the efficient insertion of transportation service data into the database tables. It provides functions to insert data from pandas DataFrames, which typically represent processed transportation service data, into the database tables. To enhance performance, data insertion is performed in batches, minimizing overhead and maximizing throughput.

5. Main Script (UberDataFiles.py):
The UberDataFiles.py script serves as the main entry point and orchestrates the entire data processing workflow. It guides users through the configuration process, gathers database connection details, specifies data sources, initiates data extraction, processes retrieved data, creates database tables, and inserts processed data into the database. This script encapsulates the end-to-end data processing pipeline for transportation service data.




Usage 🚀

1. Setup:

    Clone the repository to your local machine.
    Ensure Python is installed (version 3.6 or higher) along with the required dependencies listed in requirements.txt.

2. Configuration:

    Open UberDataFiles.py and modify the script to specify the URL or API endpoint for fetching transportation service data files.
    Provide database connection details, including DBMS type, server, database name, username, and password.
    Note: For connection with the MSSQL server, ensure that a database and user are created in the MSSQL server for successful connection.

3. Execution:

    Run the UberDataFiles.py script to initiate the data processing pipeline.
    Follow the on-screen prompts to specify required data parameters such as the year and month of data to process.

4. Output:

    Upon execution, the script will download, process, and store transportation service data into the specified database tables.




Dependencies 📦

1. Python 3.6+: The programming language used for development.

2. pandas: A powerful data manipulation library for Python, used for handling and processing tabular data.

3. requests: A library for making HTTP requests, utilized for fetching data files from web sources.

4. BeautifulSoup (bs4): A library for parsing HTML and XML documents, employed for web scraping and extracting data from web pages.

5. pyodbc: A Python library for connecting to and interacting with Microsoft SQL Server databases.

6. cx_Oracle: A Python extension module that enables access to Oracle Database.

7. psycopg2: A PostgreSQL adapter for Python, facilitating database connectivity and interaction.


📄 Business Requirement Document (BRD)

    For a comprehensive overview of the project, including business objectives, scope, and detailed specifications, please refer to the Transportation Service Performance Analysis.pdf provided. 
    The BRD outlines the project's entire lifecycle, from inception to completion.

🗒️ Note:
    
    The changes in the project and completion of further phases will be updated here

