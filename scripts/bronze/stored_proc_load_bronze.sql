/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA LOCAL INFILE` command to load data from CSV files 
      into the bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Notes:
    - Ensure the MySQL server has 'local_infile' enabled.
    - File paths must be accessible to the MySQL client executing this procedure.

Usage Example:
    CALL sp_build_silver_layer();
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE 
BEGIN
-- Start time
SET @start_time = NOW();

# load customer table
TRUNCATE TABLE bronze_crm_cust_info;
LOAD DATA LOCAL INFILE 'C:\\Users\\Mansi\\OneDrive\\Documents\\Source_crm\\cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



# load product table
TRUNCATE TABLE bronze_crm_prd_info;
LOAD DATA LOCAL INFILE "C:\\Users\\Mansi\\OneDrive\\Documents\\Source_crm\\prd_info.csv"
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



# load sales table
TRUNCATE TABLE bronze_crm_sales_details;
LOAD DATA LOCAL INFILE "C:\\Users\\Mansi\\OneDrive\\Documents\\Source_crm\\sales_details.csv"
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


# load cust az12
TRUNCATE TABLE  bronze_erp_cust_az12;
LOAD DATA LOCAL INFILE "C:\\Users\\Mansi\\OneDrive\\Documents\\Source_erp\\CUST_AZ12.csv"
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



# load location
TRUNCATE TABLE bronze_erp_loc_a101;
LOAD DATA LOCAL INFILE "C:\\Users\\Mansi\\OneDrive\\Documents\\Source_erp\\LOC_A101.csv"
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- load table bronze_erp_cust_az12
TRUNCATE TABLE bronze_erp_cust_az12;
LOAD DATA LOCAL INFILE "C:\\Users\\Mansi\\OneDrive\\Documents\\Source_erp\\CUST_AZ12.csv"
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SET @end_time = NOW();
SELECT TIMEDIFF(@end_time, @start_time) AS total_duration;
END$$

DELIMITER ;
