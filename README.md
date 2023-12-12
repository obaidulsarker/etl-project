# ETL Pipeline
This project ingest data from sharepoint file systems (csv, excels) into data warehous schema. Following tools and technologies are used here- <br>
-- Data Warehouse: PostgreSQL <br>
-- Python: Data processing and DAG files <br>
-- Airflow: Data pipeline archrastration and scheduling tools <br>

## main components:

`Extract.py` : Fetches all the Excel Files from the Directory and creates a list of filtered worksheets with "Dataset_" Prefix.

`Transform.py` : Includes the transformations to be carried out.

`Load.py` : Reads the Excel file into a dataframe and pushes the DataFrame to SQL DB.
