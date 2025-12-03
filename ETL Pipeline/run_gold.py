import duckdb

con = duckdb.connect("warehouse.db")

con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

gold_files = [
    "models/gold/gold_dim_customer.sql",
    "models/gold/gold_dim_products.sql",
    "models/gold/gold_fact_sales.sql"
]

for file in gold_files:
    print(f"Running {file}...")
    with open(file, "r") as f:
        con.execute(f.read())

print("Gold layer completed.")