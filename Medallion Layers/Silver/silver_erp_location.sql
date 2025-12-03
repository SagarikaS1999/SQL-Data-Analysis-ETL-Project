CREATE OR REPLACE TABLE silver.erp_loc_a101 AS
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry  -- Normalize and handle missing or blank country codes
FROM bronze_erp_loc_a101;


-- -- Quality Checks
-- -- It includes checks for:
-- -- Data Standardization & Consistency
-- SELECT DISTINCT 
--     cntry 
-- FROM silver_erp_loc_a101
-- ORDER BY cntry;