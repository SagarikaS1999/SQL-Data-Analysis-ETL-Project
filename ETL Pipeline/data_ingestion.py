import duckdb
import pandas as pd
import os

CRM_RAW_DIR = "data/raw/CRM Data"
ERP_RAW_DIR = "data/raw/ERP Data"

def ingest_files():
    con = duckdb.connect("warehouse.db")

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

    for file in os.listdir(CRM_RAW_DIR):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "").lower()   # e.g. customer_info.csv → customer_info
            df = pd.read_csv(f"{CRM_RAW_DIR}/{file}")
            print(f"Ingesting {file} into bronze table {table_name} ...")
            con.execute(f"CREATE OR REPLACE TABLE bronze.crm_{table_name} AS SELECT * FROM df")

    for file in os.listdir(ERP_RAW_DIR):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "").lower()   # e.g. customer_info.csv → customer_info
            df = pd.read_csv(f"{ERP_RAW_DIR}/{file}")
            print(f"Ingesting {file} into bronze table {table_name} ...")
            con.execute(f"CREATE OR REPLACE TABLE bronze.erp_{table_name} AS SELECT * FROM df")      

    print(con.execute("PRAGMA database_list").fetchall())  

    con.close()

if __name__ == "__main__":
    ingest_files()