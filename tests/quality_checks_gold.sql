/*
===============================================================================
Gold Layer Quality Checks
===============================================================================
Script Purpose:
    This script validates the Gold Layer (facts & dimensions) to ensure 
    integrity, consistency, and readiness for reporting/analytics.

Key Checks:
    - Referential integrity between Fact and Dimension tables.
    - No duplicates in dimension keys.
    - No missing surrogate keys in Fact tables.
    - Numeric/aggregated measures validation (sales, price, quantity).
    - Date consistency in reporting tables.
    - Data completeness checks (row counts vs Silver layer).

Usage Notes:
    - Run after loading the Gold Layer.
    - Fix issues in Silver Layer ETL before reloading Gold.
===============================================================================
*/

-- =====================================================
-- 1. Referential Integrity Check (Fact → Dimension)
-- =====================================================
-- Ensure all foreign keys in fact_sales exist in dimension tables
SELECT f.order_number, f.product_key, f.customer_key
FROM gold_fact_sales f
LEFT JOIN gold_dim_products p ON f.product_key = p.product_key
LEFT JOIN gold_dim_customers c ON f.customer_key = c.customer_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

-- =====================================================
-- 2. Duplicate Checks in Dimension Tables
-- =====================================================
SELECT product_key, COUNT(*)
FROM gold_dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

SELECT customer_key, COUNT(*)
FROM gold_dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- =====================================================
-- 3. Missing Key Check in Facts
-- =====================================================
-- Detect fact records without valid dimension keys
SELECT *
FROM gold_fact_sales
WHERE product_key IS NULL OR customer_key IS NULL;

-- =====================================================
-- 4. Measure Validation
-- =====================================================
-- Sales = Quantity * Price (must hold true in Gold)
SELECT *
FROM gold_fact_sales
WHERE ROUND(sales_amount, 2) != ROUND(sales_quantity * price, 2);

-- =====================================================
-- 5. Date Consistency Check
-- =====================================================
-- Ensure order_date <= shipping_date <= due_date
SELECT *
FROM gold_fact_sales
WHERE order_date > shipping_date OR shipping_date > due_date;

-- =====================================================
-- 6. Completeness Check (Row Counts vs Silver Layer)
-- =====================================================
-- Compare fact table row counts between Silver and Gold
SELECT 
    (SELECT COUNT(*) FROM silver_crm_sales_details) AS silver_count,
    (SELECT COUNT(*) FROM gold_fact_sales) AS gold_count;

-- =====================================================
-- Delete records with missing customer keys in Gold Fact
-- Only use if these records cannot be fixed by mapping to dimension
-- =====================================================
DELETE FROM gold_fact_sales
WHERE customer_key IS NULL;
