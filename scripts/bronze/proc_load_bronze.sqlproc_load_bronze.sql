/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Script Purpose:
    This stored procedure automates the **Bronze Layer loading process** 
    in the SQL Data Warehouse. It performs a full truncate and reload 
    for all CRM and ERP source tables using `BULK INSERT` from CSV files 
    located in the local dataset directories.

Process Summary:
    1. Prints start message and timestamps
    2. Truncates each target table under the `bronze` schema
    3. Loads fresh data into each table using `BULK INSERT`
    4. Logs time taken for each table load
    5. Handles errors gracefully with try-catch and error logging
    6. Prints total batch load duration for end-to-end visibility

Tables Loaded:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2

Execution Note:
    - Make sure the CSV source files are accessible at the specified paths
    - Intended for use in development or refresh of staging data (Bronze Layer)
    - Designed for SQL Server with support for `BULK INSERT`, `PRINT`, and error handling

Author: [Your Name]
Created On: [Date]
===============================================================================
*/


create or alter procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '================================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '================================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------';

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>> Inserting Data Into: bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT '>> Inserting Data Into: bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT '>> Inserting Data Into: bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------';

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>> Inserting Data Into: bronze.erp_cust_az12';	
			BULK INSERT bronze.erp_cust_az12
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'D:\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			SET @batch_end_time = GETDATE();
			PRINT '=========================================='
			PRINT 'Loading Bronze Layer is Completed';
			PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=========================================='
		END TRY
		BEGIN CATCH
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
		END CATCH
END 
