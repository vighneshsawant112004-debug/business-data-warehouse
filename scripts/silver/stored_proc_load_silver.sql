/*
===============================================================================
Stored Procedure: Build Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure transforms and loads data into the 'silver' schema 
    from the 'bronze' schema.  
    It performs the following actions:
    - Truncates the silver tables before inserting data.
    - Cleans, standardizes, and transforms data using SQL queries.
    - Applies business rules (e.g., normalizing gender, marital status, dates).
    - Prepares clean, analytics-ready tables for further use in the Gold layer.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Notes:
    - If the stored procedure fails or is not supported in your MySQL environment,
      run the transformation scripts manually (INSERT...SELECT statements).
    - Ensure all Bronze layer tables are fully loaded before running this procedure.

Usage Example:
sp_build_silver_layer();
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE sp_build_silver_layer()
BEGIN

truncate table silver_crm_cust_info;
INSERT INTO silver_crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze_crm_cust_info
			WHERE cst_id IS NOT NULL
		) t;
        
select * from silver_crm_cust_info;


-- ==============================================================================
-- Build Silver Layer: CRM Product info
-- =========================================================================
truncate table  silver_crm_prd_info;
insert into silver_crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt_test
)
select 
prd_id,
replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- adding new column as category id or this category connect with bronze_erp_px_cat_g1v2 id column
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
prd_cost,
CASE prd_line
    WHEN upper(trim(prd_line)) = 'M' THEN 'Mountain'
    WHEN upper(trim(prd_line)) ='R' THEN 'Road'
    WHEN upper(trim(prd_line)) = 'S' THEN 'Other Sales'
    WHEN upper(trim(prd_line)) = 'T' THEN 'Touring'
    ELSE 'N/A'
  END AS prd_line,
cast(prd_start_dt as date ) as prd_start_dt,
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as prd_end_dt_test
from bronze_crm_prd_info;


-- ==============================================================================
-- Build Silver Layer: CRM Sales Details
-- =========================================================================
truncate table  silver_crm_sales_details;
INSERT INTO silver_crm_sales_details (
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
END AS sls_order_dt,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
END AS sls_ship_dt,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
END AS sls_due_dbt,
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
from bronze_crm_sales_details
;
-- ==============================================================================
-- Build Silver Layer: erp_cust_az12
-- =============================================================================
truncate table  silver_erp_cust_az12;
insert into silver_erp_cust_az12(cid,bdate,gen)
select 
case when cid like 'NAS%' then substring(cid,4,length(cid))
else cid
end cid,
case when bdate > curdate() then null
else bdate
end as bdate,
 CASE 
        WHEN gen IS NULL 
             OR TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), '')) = '' 
            THEN 'Unknown'

        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) 
             IN ('M','MALE') 
            THEN 'Male'

        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) 
             IN ('F','FEMALE') 
            THEN 'Female'

        ELSE TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))
    END AS gen
from bronze_erp_cust_az12;


-- ==============================================================================
-- Build Silver Layer: erp_loc_a101
-- =============================================================================
truncate table  silver_erp_loc_a101;
insert into silver_erp_loc_a101 (cid,cntry)
select 
replace(cid,'-','') cid,  
 CASE 
        WHEN cntry IS NULL OR TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' 
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
    END AS cntry
from bronze_erp_loc_a101;

-- ==============================================================================
-- Build Silver Layer: erp_px_cat_g1v2
-- =============================================================================
truncate table  silver_erp_px_cat_g1v2;
insert into silver_erp_px_cat_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze_erp_px_cat_g1v2;
END $$

DELIMITER ;
