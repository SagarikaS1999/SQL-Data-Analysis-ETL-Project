CREATE OR REPLACE TABLE silver.erp_px_cat_g1v2  AS
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze_erp_px_cat_g1v2;

-- -- Check for Unwanted Spaces
-- SELECT 
--     * 
-- FROM bronze_erp_px_cat_g1v2
-- WHERE cat != TRIM(cat) 
--    OR subcat != TRIM(subcat) 
--    OR maintenance != TRIM(maintenance);