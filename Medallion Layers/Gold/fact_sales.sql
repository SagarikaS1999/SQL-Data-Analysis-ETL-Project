CREATE OR REPLACE TABLE gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
    sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver_crm_sales_details sd
LEFT JOIN gold_dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

-- Check if all dimension tables can successfully join to the fact table
-- -- Foreign key integrity (dimensions)
-- SELECT * 
-- FROM gold_fact_sales f
-- LEFT JOIN gold_dim_customers c
-- ON c.customer_key = f.customer_key
-- LEFT JOIN gold_dim_products p
-- ON p.product_key = f.product_key
-- WHERE c.customer_key IS NULL;