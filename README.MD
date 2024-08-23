# Football Stats ETL Pipeline

This project involves fetching football statistics from an API, processing the data, and loading it into a data warehouse for analysis. The pipeline includes data storage, transformation, and orchestration components.

## Project Overview

1. **Data Extraction**
   - Fetches football statistics data from a football statistics API.

2. **Data Storage**
   - Stores the fetched data in Parquet format in S3 (MinIO).

3. **Data Loading**
   - Uses PXF to load data from S3 into Greenplum as external tables.

4. **Data Transformation**
   - Applies the Data Vault methodology to transform the data into the main data layer using DBT with the AutomateDV package.
   - Creates a data mart layer for further analysis.

5. **Orchestration**
   - Orchestrates the entire pipeline using Airflow.
   - Utilizes Astronomer Cosmos for running DBT tasks.

## Components

- **Football Statistics API**: Source of football data.
- **MinIO**: Object storage for saving data in Parquet format.
- **Greenplum**: Data warehouse for staging and processing data.
- **PXF**: Connector for loading data from S3 to Greenplum.
- **DBT (AutomateDV)**: For data transformation and Data Vault modeling.
- **Airflow**: Workflow orchestration.
- **Astronomer Cosmos**: For managing and executing DBT runs.