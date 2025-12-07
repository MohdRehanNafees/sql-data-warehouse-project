/*
=======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=======================================================================
Script Purpose:
  Automates the loading of raw CRM and ERP data into the **Bronze layer** of the data warehouse. 
  It truncates existing tables, bulk-loads fresh CSV data, tracks load duration per table, and logs 
  total execution time. Includes error handling for monitoring failures.

Parameters:
  This procedure does **not take any parameters**. All file paths and tables are pre-defined within the procedure.

Usage Example:
  Execute the Bronze layer load
  EXEC bronze.load_bronze;
  Running this command will refresh all Bronze tables with the latest raw data from the source CSV files.

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME,
            @total_start DATETIME,
            @total_end DATETIME;

    SET @total_start = GETDATE();  -- ⬅ Start total timer

    BEGIN TRY
        PRINT '=======================';
        PRINT 'Loading Bronze Layer';
        PRINT '=======================';
        
        -----------------------------------
        -- CRM TABLES
        -----------------------------------
        PRINT '-----------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------';

        -----------------------------------
        -- crm_cust_info
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- crm_prd_info
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- crm_sales_details
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- ERP TABLES
        -----------------------------------
        PRINT '-----------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------';

        -----------------------------------
        -- erp_cust_az12
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- erp_loc_a101
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- erp_px_cat_g1v2
        -----------------------------------
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\rehan\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------';

        -----------------------------------
        -- PRINT TOTAL DURATION
        -----------------------------------
        SET @total_end = GETDATE();
        PRINT '========================================';
        PRINT 'TOTAL BRONZE LAYER LOAD TIME: ' 
              + CAST(DATEDIFF(SECOND, @total_start, @total_end) AS NVARCHAR) 
              + ' seconds';
        PRINT '========================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '========================================';
    END CATCH
END
