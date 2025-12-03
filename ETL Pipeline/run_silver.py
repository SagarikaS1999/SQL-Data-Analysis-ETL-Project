import duckdb

con = duckdb.connect("warehouse.db")

con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

silver_files = [
    "models/silver/silver_crm_customer.sql",
    "models/silver/silver_crm_products.sql",
    "models/silver/silver_crm_sales.sql",
    "models/silver/silver_erp_customer.sql",
    "models/silver/silver_erp_categories.sql",
    "models/silver/silver_erp_location.sql"
]

for file in silver_files:
    print(f"Running {file}...")
    with open(file, "r") as f:
        sql = f.read()
        con.execute(sql)

print("Silver layer completed.")
