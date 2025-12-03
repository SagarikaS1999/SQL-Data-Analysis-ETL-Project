from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # 1. Start a Spark session (local mode)
    spark = (
        SparkSession.builder
        .appName("CustomerSegmentation")
        .master("local[*]")  # use all local cores
        .getOrCreate()
    )

    # 2. Read fact_sales and dim_customer exported from DuckDB
    fact_path = "exports/gold_fact_sales.csv"
    cust_path = "exports/gold_dim_customer.csv"

    fact_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(fact_path)
    )

    cust_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(cust_path)
    )

    # 3. Join fact + dim on customer_key
    joined = (
        fact_df.alias("f")
        .join(
            cust_df.alias("c"),
            on=F.col("f.customer_key") == F.col("c.customer_key"),
            how="inner"
        )
    )

    # 4. Aggregate: total revenue + orders per customer
    customer_agg = (
        joined.groupBy(
            "c.customer_key",
            "c.customer_id",
            "c.customer_number",
            "c.first_name",
            "c.last_name",
            "c.country"
        )
        .agg(
            F.sum("f.sales_amount").alias("total_sales_amount"),
            F.sum("f.quantity").alias("total_units"),
            F.countDistinct("f.order_number").alias("total_orders")
        )
    )

    # 5. Compute revenue percentiles and assign segments: High / Medium / Low
    quantiles = customer_agg.approxQuantile("total_sales_amount", [0.33, 0.66], 0.01)
    low_cutoff, high_cutoff = quantiles[0], quantiles[1]

    customer_segmented = (
        customer_agg.withColumn(
            "value_segment",
            F.when(F.col("total_sales_amount") >= high_cutoff, F.lit("High"))
             .when(F.col("total_sales_amount") >= low_cutoff, F.lit("Medium"))
             .otherwise(F.lit("Low"))
        )
    )

    # 6. Optional: order for readability
    customer_segmented = customer_segmented.orderBy(F.desc("total_sales_amount"))

    # 7. Write the result out as CSV (to use in Power BI or just show as output)
    output_path = "exports/spark_customer_segments"
    (
        customer_segmented
        .coalesce(1)  # single file for convenience
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    print(f"Customer segmentation written to {output_path}/")

    spark.stop()


if __name__ == "__main__":
    main()



#     “In addition to SQL-based modeling, I also implemented a small PySpark module to show how this pipeline could scale in a distributed environment.
# Here I load the fact_sales and dim_customer tables, compute total revenue per customer, and then apply percentile-based segmentation into high-, medium-, and low-value customers.
# This Spark job writes the segmented customers back as a CSV that can feed downstream analytics or reporting.”