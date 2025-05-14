/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

USE DataWarehouse
Go

-- To save this repeatative procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	
	-- Declare time variable to check how much tiem it take to load data in bronze layer
	DECLARE @start_time DATETIME, @end_time DATETIME -- for table time
	DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME -- for Bronze layer time

		-- To handle errror
		BEGIN TRY
			SET @start_time_batch = GETDATE()

				PRINT '======================================================'
				PRINT 'Loading bronze layer........'
				PRINT '======================================================'

				PRINT '======================================================'
				PRINT 'Loading CRM Table........'
				PRINT '======================================================'

				--------------------------------------------------------------

				SET @start_time = GETDATE()
				-- Truncate is use to empty the table with previus data
				PRINT 'Truncating Table : bronze.crm_cust_info'
				TRUNCATE TABLE bronze.crm_cust_info

				PRINT 'Inserting Table into bronze.crm_cust_info'
				-- Bulk insert from local files
				BULK INSERT bronze.crm_cust_info

				-- Select path
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'

				-- how files is going to be
				WITH (
					FIRSTROW = 2,-- skip the 1st row as it contains header
					FIELDTERMINATOR = ',', -- How the values are seperated
					TABLOCK -- TABLOCK tells SQL Server to place a lock on the entire table
				)
				SET @end_time = GETDATE()
			
				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				--------------------------------------------------

				SET @start_time = GETDATE()
				PRINT 'Truncating Table : bronze.crm_prd_info'
				TRUNCATE TABLE bronze.crm_prd_info

				PRINT 'Inserting Table into bronze.crm_prd_info'
				BULK INSERT bronze.crm_prd_info
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
				SET @end_time = GETDATE()

				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				----------------------------------------------------

				SET @start_time = GETDATE()
				PRINT 'Truncating Table : bronze.crm_sales_details'
				TRUNCATE TABLE bronze.crm_sales_details

				PRINT 'Inserting Table into bronze.crm_sales_details'
				BULK INSERT bronze.crm_sales_details
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK 
				)
				SET @end_time = GETDATE()

				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				------------------------------

				PRINT '======================================================'
				PRINT 'Loading ERP Table........'
				PRINT '======================================================'

				SET @start_time = GETDATE()
				PRINT 'Truncating Table : bronze.erp_CUST_AZ12'
				TRUNCATE TABLE bronze.erp_CUST_AZ12

				PRINT 'Inserting Table into bronze.erp_CUST_AZ12'
				BULK INSERT bronze.erp_CUST_AZ12
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK 
				)
				SET @end_time = GETDATE()

				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				------------------------------

				SET @start_time = GETDATE()
				PRINT 'Truncating Table : bronze.erp_LOC_A101'
				TRUNCATE TABLE bronze.erp_LOC_A101

				PRINT 'Inserting Table into bronze.erp_LOC_A101'
				BULK INSERT bronze.erp_LOC_A101
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK 
				)
				SET @end_time = GETDATE()

				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				------------------------------

				SET @start_time = GETDATE()
				PRINT 'Truncating Table : bronze.erp_PX_CAT_G1V2'
				TRUNCATE TABLE bronze.erp_PX_CAT_G1V2

				PRINT 'Inserting Table into bronze.erp_PX_CAT_G1V2'
				BULK INSERT bronze.erp_PX_CAT_G1V2
				FROM 'C:\Career\Applying jobs\Addepar\ELT PRoject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK 
				)
				SET @end_time = GETDATE()

				PRINT '>> Loadimg time taken ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
				PRINT '--------------------------'
				----------------------------------------------------
				SET @end_time_batch = GETDATE()

				PRINT '======================================================'
				PRINT 'Loading bronze Duration : ' + CAST(DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR) + 'Seconds' 
				PRINT '======================================================'
				

				----------------------------------------------------

				SELECT COUNT(*) FROM bronze.crm_cust_info
				SELECT COUNT(*) FROM bronze.crm_prd_info
				SELECT COUNT(*) FROM bronze.crm_sales_details
				SELECT COUNT(*) FROM bronze.erp_CUST_AZ12
				SELECT COUNT(*) FROM bronze.erp_LOC_A101
				SELECT COUNT(*) FROM bronze.erp_PX_CAT_G1V2


		END TRY
	
		-- IF TRY fails, catch will work
		BEGIN CATCH
			PRINT '======================================================'
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE()
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR)
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
			PRINT '======================================================'
		END CATCH
END
