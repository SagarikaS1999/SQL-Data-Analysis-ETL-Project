CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    ci.customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname As last_name,
    la.cntry AS country,
    ci.marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM data is the master information
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.create_date
FROM silver_crm_customer_info ci
LEFT JOIN silver_erp_cust_az12 ca
ON 
    ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la
ON
    ci.cst_key = la.cid;  