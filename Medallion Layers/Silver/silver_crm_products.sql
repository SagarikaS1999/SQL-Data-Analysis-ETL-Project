CREATE OR REPLACE TABLE silver.crm_product_info AS
WITH silver_crm_product AS (
    SELECT
        prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key_clean,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE                                   -- Map product line codes to descriptive values
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(strptime(prd_start_dt, '%Y-%m-%d') AS DATE) AS prd_start_dt
    FROM bronze_crm_products_info
)
SELECT
    *,
    (
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
        - INTERVAL '1' DAY
    )::DATE AS prd_end_dt -- Calculate end date as one day before the next start date
FROM silver_crm_product;


-- Quality Checks
-- It includes checks for:
    -- Null or duplicate primary keys.
    -- Unwanted spaces in string fields.
    -- Data standardization and consistency.
    -- Invalid date ranges and orders.
    -- Data consistency between related fields.

-- -- Checking for NULLs or duplicates in primary key
-- SELECT 
--     prd_id,
--     COUNT(*) 
-- FROM silver_crm_product_info
-- GROUP BY prd_id
-- HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- -- Checking for unwanted spaces
-- SELECT 
--     prd_nm 
-- FROM silver_crm_product_info
-- WHERE prd_nm != TRIM(prd_nm);

-- -- Checking for NULLs or negative values in cost
-- SELECT 
--     prd_cost 
-- FROM silver_crm_product_info
-- WHERE prd_cost < 0 OR prd_cost IS NULL;

-- -- Checking standardization & consistency
-- SELECT DISTINCT 
--     prd_line 
-- FROM silver_crm_product_info;

-- -- Checking for invalid date orders (Start Date > End Date)
-- SELECT 
--     * 
-- FROM silver_crm_product_info
-- WHERE prd_end_dt < prd_start_dt;