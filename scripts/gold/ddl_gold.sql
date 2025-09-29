/*
===============================================================================
Stored Procedure: Build Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure aggregates and models data into the 'gold' schema 
    from the 'silver' schema.  
    It performs the following actions:
    - Truncates the gold tables before inserting data.
    - Applies business logic for KPIs and metrics (e.g., sales revenue, 
      customer lifetime value, product performance).
    - Structures data into analytics-ready formats (e.g., star schema fact 
      and dimension tables).
    - Prepares final outputs for BI tools such as Power BI or Tableau.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Notes:
    - Ensure all Silver layer transformations are completed successfully before 
      running this procedure.
    - If the stored procedure fails or is not supported in your MySQL environment,
      run the aggregation and modeling scripts manually (INSERT...SELECT statements).
    - Gold layer tables are intended for business consumption and reporting.

Usage Example:
    CALL build_gold();
===============================================================================
*/

-- ============================================================================
-- Gold Layer: Dimension - Customers
-- ============================================================================
DROP VIEW IF EXISTS gold_dim_customers;

CREATE VIEW gold_dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM silver_crm_cust_info ci
LEFT JOIN silver_erp_cust_az12 ca 
       ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la 
       ON ci.cst_key = la.cid;

-- ============================================================================
-- Gold Layer: Dimension - Products
-- ============================================================================
DROP VIEW IF EXISTS gold_dim_products;

CREATE VIEW gold_dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc 
       ON pn.cat_id = pc.id
WHERE pn.prd_end_dt_test IS NULL; -- filter to only active (non-historical) products

-- ============================================================================
-- Gold Layer: Fact - Sales
-- ============================================================================
DROP VIEW IF EXISTS gold_fact_sales;

CREATE VIEW gold_fact_sales AS
SELECT 
    ps.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    ps.sls_order_dt AS order_date,
    ps.sls_ship_dt AS shipping_date,
    ps.sls_due_dt AS due_date,
    ps.sls_sales AS sales_amount,
    ps.sls_quantity AS sales_quantity,
    ps.sls_price AS price
FROM silver_crm_sales_details ps
LEFT JOIN gold_dim_products pr 
       ON ps.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers cu 
       ON ps.sls_cust_id = cu.customer_id;

 
