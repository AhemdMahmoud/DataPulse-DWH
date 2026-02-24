/*
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
    exec bronze.load_bronze;
===============================================================================
*/


use DataWarehouse
go

create or alter procedure  bronze.load_bronze as
begin
	declare @start_time Datetime,@end_time Datetime , @batch_start_time Datetime,@batch_end_time Datetime
	begin try 
	Set @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'loading Bronze Layer';
		PRINT '================================================';


		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables 🫡😂❤️🫡';
		PRINT '------------------------------------------------';

		set @start_time = GetDate();
		PRINT '>> Truncating Table: bronze.crm_cust_info if Contain Data to Prevent Dublicate';
		truncate table bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk insert bronze.crm_cust_info
		from '/var/opt/mssql/backup/P_datasets/source_crm/cust_info.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 )
		 PRINT '>> Data Loaded Successfully ❤️';

		set @end_time = GetDate();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

	print '------------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info if Contain Data to Prevent Dublicate';

		truncate table bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';

		Bulk insert bronze.crm_prd_info
		from '/var/opt/mssql/backup/P_datasets/source_crm/prd_info.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 )
		 PRINT '>> Data Loaded Successfully ❤️';
		  SET @end_time = GETDATE();
		  print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';


print '------------------------------------------------'
		set @start_time = GetDate();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';

		Bulk insert bronze.crm_sales_details
		from '/var/opt/mssql/backup/P_datasets/source_crm/sales_details.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 )
		 PRINT '>> Data Loaded Successfully ❤️';
		 set @end_time = GetDate();
		 print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

		
print '------------------------------------------------'


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

		Bulk insert bronze.erp_loc_a101
		from '/var/opt/mssql/backup/P_datasets/source_erp/loc_a101.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 )
		 PRINT '>> Data Loaded Successfully ❤️';

		SET @end_time = GETDATE();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

print '------------------------------------------------'
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		truncate  table bronze.erp_cust_az12

		

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

		Bulk insert bronze.erp_cust_az12
		from '/var/opt/mssql/backup/P_datasets/source_erp/cust_az12.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 )
		 PRINT '>> Data Loaded Successfully ❤️';
		 set @end_time = GetDate();
		 print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

	print '------------------------------------------------'	
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk insert bronze.erp_px_cat_g1v2
		from '/var/opt/mssql/backup/P_datasets/source_erp/px_cat_g1v2.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		 );
		 PRINT '>> Data Loaded Successfully ❤️';

		 SET @end_time = GETDATE();
		 print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';
print '------------------------------------------------'

	Set @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Bronze Layer is Completed';
    PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '-- All processes were completed successfully.'

	END TRY

		Begin Catch 
		print ' -----------------------------------------------'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message : ' + error_message();
		print 'Error Number : ' + cast(error_number() as Nvarchar);
		print 'Error Status : ' + cast(error_state() as Nvarchar);
		PRINT '=========================================='

	End Catch 

end;


-- test 
exec bronze.load_bronze

select count(*) from bronze.erp_loc_a101
