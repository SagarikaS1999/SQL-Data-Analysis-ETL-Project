CREATE OR REPLACE TABLE silver.crm_sales_details AS
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
		ELSE CAST(strptime(sls_order_dt::VARCHAR, '%Y%m%d') AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
		ELSE CAST(strptime(sls_ship_dt::VARCHAR, '%Y%m%d') AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
		ELSE CAST(strptime(sls_due_dt::VARCHAR, '%Y%m%d') AS DATE)
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)   --- Business Rule: Sales = Quantity * Price or negavtive
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
FROM bronze_crm_sales_details;


-- -- Checking for invalid dates
-- SELECT 
--     NULLIF(sls_due_dt, 0) AS sls_due_dt 
-- FROM bronze_crm_sales_details
-- WHERE sls_due_dt <= 0 
--     OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8   --length of date must be 8
--     OR sls_due_dt > 20500101  --checking for outliers by validating the boundaries of the date range
--     OR sls_due_dt < 19000101;

-- -- Checking for invalid date orders (Order Date > Shipping/Due Dates)
-- SELECT 
--     * 
-- FROM silver_crm_sales_details
-- WHERE sls_order_dt > sls_ship_dt 
--    OR sls_order_dt > sls_due_dt;

-- -- Checking data consistency: Sales = Quantity * Price or negative number or less than 0's
-- SELECT DISTINCT 
--     sls_sales,
--     sls_quantity,
--     sls_price 
-- FROM silver_crm_sales_details
-- WHERE sls_sales != sls_quantity * sls_price
--    OR sls_sales IS NULL 
--    OR sls_quantity IS NULL 
--    OR sls_price IS NULL
--    OR sls_sales <= 0 
--    OR sls_quantity <= 0 
--    OR sls_price <= 0
--    AND sls_quantity != 1
-- ORDER BY sls_sales, sls_quantity, sls_price;