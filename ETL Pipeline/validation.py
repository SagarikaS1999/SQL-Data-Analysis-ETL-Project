import pandas as pd
import os

CRM_RAW_DIR = "data/raw/CRM Data"
ERP_RAW_DIR = "data/raw/ERP Data"

def validate_csv(path):
    df = pd.read_csv(path)

    # Basic checks
    if df.empty:
        raise ValueError(f"File {path} is empty")

    print(f"{path}: {len(df)} rows")

    # Column sanity check
    if df.isnull().sum().sum() > 0:
        print(f"WARNING: Null values found in {path}")

    # Duplicates
    if df.duplicated().any():
        print(f"WARNING: Duplicate rows found in {path}")

def run_crm_validation():
    for file in os.listdir(CRM_RAW_DIR):
        if file.endswith(".csv"):
            print("\nChecking:", file)
            validate_csv(f"{CRM_RAW_DIR}/{file}")

def run_erp_validation():
    for file in os.listdir(ERP_RAW_DIR):
        if file.endswith(".csv"):
            print("\nChecking:", file)
            validate_csv(f"{ERP_RAW_DIR}/{file}")

if __name__ == "__main__":
    run_crm_validation()
    run_erp_validation()