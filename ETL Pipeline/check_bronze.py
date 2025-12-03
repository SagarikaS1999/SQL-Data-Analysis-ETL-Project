import duckdb

con = duckdb.connect("warehouse.db")

print("\n=== ALL TABLES IN DUCKDB (Bronze Layer) ===")
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
""").fetchall()
print(tables)

print("\n=== SAMPLE ROWS FROM EACH TABLE ===")
for (table_name,) in tables:
    print(f"\n--- bronze.{table_name} ---")
    try:
        df = con.execute(f"SELECT * FROM bronze.{table_name} LIMIT 5").fetchdf()
        print(df)
    except Exception as e:
        print(f"Error reading {table_name}: {e}")