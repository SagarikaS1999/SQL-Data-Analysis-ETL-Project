CREATE OR REPLACE TABLE silver.crm_customer_info AS
SELECT 
    CAST(cst_id AS INTEGER) AS customer_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, 
    TRIM(cst_lastname) AS cst_lastname,   --checking for unwanted spaces
     CASE                                           -- data standardization and consistency
        WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE cst_marital_status
    END AS marital_status,
    CASE                                    
        WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    CAST(strptime(cst_create_date, '%m/%d/%Y') AS DATE) AS create_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
    FROM
        bronze_crm_customer_info
) t
WHERE flag_last = 1; -- Select the most recent record per customer


-- Quality Checks
-- It includes checks for:
    -- Null or duplicate primary keys.
    -- Unwanted spaces in string fields.
    -- Data standardization and consistency.
    -- Invalid date ranges and orders.
    -- Data consistency between related fields.

-- -- Checking for NULLs or duplicates in primary key
-- SELECT 
--     cst_id,
--     COUNT(*) 
-- FROM silver_crm_customer_info
-- GROUP BY cst_id
-- HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- -- Checking for unwanted spaces
-- SELECT 
--     cst_key 
-- FROM silver_crm_customer_info
-- WHERE cst_key != TRIM(cst_key);

-- -- Checking data standardization and consistency
-- SELECT DISTINCT 
--     cst_marital_status 
-- FROM silver_crm_customer_info;