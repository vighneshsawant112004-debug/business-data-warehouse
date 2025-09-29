/*
===============================================================================
Data Quality & Profiling Checks
===============================================================================

Script Purpose:
    This script performs **data quality checks** on the Bronze Layer tables 
    to ensure data consistency, accuracy, and standardization before promoting 
    data into the Silver Layer.  

    Key Checks:
        - Null or duplicate primary keys.
        - Unwanted spaces in string fields.
        - Standardization of categorical fields (gender, country, etc.).
        - Invalid or inconsistent date ranges.
        - Consistency between related numeric fields (e.g., sales = quantity * price).
        - Detection of out-of-range or corrupted values.

Usage Notes:
    - Run these checks immediately after data ingestion into the Bronze Layer.
    - Use the results to clean/standardize data before inserting into Silver Layer.
    - Any anomalies (duplicates, invalid dates, inconsistencies) must be reviewed, 
      corrected, or logged before further transformations.
    - Final goal: ensure Silver Layer only contains clean, reliable, and standardized data.

===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- 1. Check duplicates & nulls in primary key
SELECT 
    cst_id,
    COUNT(*)
FROM bronze_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- 2. Detect unwanted spaces in firstname
SELECT cst_firstname
FROM silver_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- 3. Detect unwanted spaces in lastname
SELECT cst_lastname
FROM bronze_crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- 4. Verify gender field for unwanted spaces
SELECT cst_gndr
FROM bronze_crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- 5. Preview cleaned firstname & lastname
SELECT 
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname
FROM bronze_crm_cust_info;

-- 6. Check distinct gender values for standardization
SELECT DISTINCT cst_gndr
FROM bronze_crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ===================================================================
-- 1. Check duplicates & nulls in primary key
SELECT 
    prd_id,
    COUNT(*)
FROM bronze_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 2. Detect unwanted spaces in product name
SELECT prd_nm
FROM bronze_crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 3. Check for nulls or negative product cost
SELECT prd_cost
FROM bronze_crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 4. Standardization check: distinct product lines
SELECT DISTINCT prd_line
FROM bronze_crm_prd_info;

-- 5. Validate date consistency (end date must be >= start date)
SELECT * 
FROM bronze_crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- 6. Review transformed (silver layer) data
SELECT * 
FROM silver_crm_prd_info;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- 1. Check for invalid dates (length mismatch, zero, or NULL)
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze_crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

-- 2. Validate logical date order (ship & due dates after order date)
SELECT * 
FROM bronze_crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- 3. Check consistency between Sales, Quantity & Price
-- Rule: Sales = Quantity * Price
SELECT DISTINCT
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR ROUND(sls_sales, 4) != ROUND(sls_quantity * ABS(sls_price), 4)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0 
        THEN ROUND(sls_sales / NULLIF(sls_quantity, 0), 4)
        ELSE sls_price
    END AS sls_price
FROM bronze_crm_sales_details
WHERE sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
   OR ROUND(sls_sales, 4) != ROUND(sls_quantity * sls_price, 4)
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify out-of-range birthdates
SELECT bdate
FROM bronze_erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM bronze_erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- 1. Distinct country codes in silver layer
SELECT DISTINCT cntry
FROM silver_erp_loc_a101;

-- 2. Identify leading/trailing spaces & string lengths
SELECT DISTINCT 
    CONCAT('[', cntry, ']') AS raw_value,
    LENGTH(cntry) AS str_length
FROM bronze_erp_loc_a101;

-- 3. Detect hidden/invalid characters (hex format)
SELECT DISTINCT
    cntry, 
    HEX(cntry) AS hex_value
FROM bronze_erp_loc_a101;

-- 4. Standardize country names
SELECT 
    cntry,
    CASE 
        WHEN cntry IS NULL 
             OR TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' 
            THEN 'Unknown'

        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) = 'DE' 
            THEN 'Germany'

        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) 
             IN ('USA','US','UNITED STATES') 
            THEN 'United States'

        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) 
             IN ('UK','UNITED KINGDOM') 
            THEN 'United Kingdom'

        ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
    END AS cleaned_country
FROM bronze_erp_loc_a101;


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- 1. Detect unwanted spaces
SELECT *
FROM bronze_erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- 2. Standardization: distinct categories
SELECT DISTINCT cat
FROM bronze_erp_px_cat_g1v2;
