CREATE OR REPLACE TABLE silver.erp_cust_az12 AS
SELECT
    CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
	    ELSE cid
	END AS cid, 
	CASE
		WHEN CAST(strptime(bdate::VARCHAR, '%Y-%m-%d') AS DATE) > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate, -- Set future birthdates to NULL
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen -- Normalize gender values and handle unknown cases
FROM bronze_erp_cust_az12;


-- -- Identifying out-of-range dates
-- -- Checking for birthdays in future
-- SELECT DISTINCT 
--     bdate 
-- FROM silver_erp_cust_az12
-- WHERE CAST(strptime(bdate::VARCHAR, '%Y-%m-%d') AS DATE) < '1924-01-01' 
--    OR CAST(strptime(bdate::VARCHAR, '%Y-%m-%d') AS DATE) > CURRENT_DATE;

-- -- Checking data standardization & consistency
-- SELECT DISTINCT 
--     gen 
-- FROM silver_erp_cust_az12;