# 🚗 Transportation Service Performance Analysis 📊

Phase 1: Data Extraction, Exploration, Cleaning, and Processing

This project facilitates the extraction, exploration, cleaning, and processing of transportation service data, enabling comprehensive analysis and performance evaluation.

## Project Structure 🛠️

### Data Extraction (DataExtractor.py):
Responsible for fetching transportation service data files from specified sources, utilizing requests library and BeautifulSoup for web scraping.

### Database Connection (DatabaseConnector.py):
Offers a flexible interface for connecting to various relational databases, supporting popular DBMSs such as Microsoft SQL Server, Oracle, and PostgreSQL.

### Table Creation (TableCreation.py):
Defines functions for generating database tables optimized for storing transportation service data.

### Table Insertion (TableInsertion.py):
Handles efficient insertion of transportation service data into the database tables.

### Main Script (UberDataFiles.py):
Serves as the main entry point, orchestrating the entire data processing workflow.

## Usage 🚀

### Setup:
- Clone the repository to your local machine.
- Ensure Python is installed (version 3.6 or higher) along with the required dependencies listed in requirements.txt.
- Open UberDataFiles.py and modify the script to specify the URL or API endpoint for fetching transportation service data files.

### Configuration:
- Provide database connection details, including DBMS type, server, database name, username, and password.
- Note: For connection with the MSSQL server, ensure that a database and user are created in the MSSQL server for successful connection.

### Execution:
- Run the UberDataFiles.py script to initiate the data processing pipeline.
- Follow the on-screen prompts to specify required data parameters such as the year and month of data to process.

### Output:
- Upon execution, the script will download, process, and store transportation service data into the specified database tables.

## Phase 2: ETL Process (Data Transformation and Loading) 🔄

This phase focuses on the Extract, Transform, Load (ETL) process, which plays a critical role in preparing data for analysis.

### Tools Utilized:
- **Informatica:** Selected for its robust ETL capabilities, allowing seamless transformation and loading of data between different databases.

### Integration:
- **Microsoft SQL Server:** Utilize as the first layer staging database.
  - The extracted data from Python generates separated tables for each month in the MSSQL database.
  - Run the SQL command file named "TSPA_22_STAGING_MSSQL" to create a unified table in MSSQL, which consolidates data from other tables. This file contains SQL commands to create the unified table and define its structure based on the data from the separate monthly tables.
- **Oracle Server:** Utilized as the second layer staging database and core database.
  - Create two separate databases in Oracle: "staging" and "core".
  - Run the SQL command file "TSPA_STAGING_ORACLE.sql" in Oracle to create the staging table in the staging database.
  - Run the SQL command file "TSPA_CORE_ORACLE.sql" in Oracle to create the core table in the core database.
  - Extract data from Microsoft SQL Server and load it into the staging database of Oracle as part of the ETL process.
  - Further transform and load data from the staging database into the "core" database for final storage.
- **Informatica Integration:**
  - Use Informatica to extract data from Microsoft SQL Server and load it into the staging database of Oracle, ensuring smooth data transformation and loading.
  - Additionally, utilize Informatica to facilitate the seamless connection between the staging database and core database for the ETL process.

## Phase 3: Final Presentation of Graphical Analysis 📈

This phase involves presenting the insights derived from the transportation service data through graphical analysis.

### Approach:
Conduct graphical analysis using a combination of data visualization tools and techniques to effectively communicate findings.

### Tools Utilized:
- **Tableau:** Leverage for its powerful visualization capabilities, enabling the creation of interactive and insightful dashboards.
- **Oracle Database:** Connect directly to Tableau for seamless extraction of data for visualization purposes.

## Dependencies 📦

- Python 3.6+: The programming language used for development.
- pandas: A powerful data manipulation library for Python, used for handling and processing tabular data.
- requests: A library for making HTTP requests, utilized for fetching data files from web sources.
- BeautifulSoup (bs4): A library for parsing HTML and XML documents, employed for web scraping and extracting data from web pages.
- pyodbc: A Python library for connecting to and interacting with Microsoft SQL Server databases.
- cx_Oracle: A Python extension module that enables access to Oracle Database.
- psycopg2: A PostgreSQL adapter for Python, facilitating database connectivity and interaction.
- Informatica: An ETL tool used for data transformation and loading.
- Tableau: A data visualization tool for creating insightful dashboards and presentations.

## 📄 Business Requirement Document (BRD)

For a comprehensive overview of the project, including business objectives, scope, and detailed specifications, please refer to the Transportation Service Performance Analysis.pdf provided. 
The BRD outlines the project's entire lifecycle, from inception to completion.

## 🗒️ Note:

The changes in the project and completion of further phases will be updated here
