/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
*/

use DataWarehouse
go 
create or alter procedure silver.load_silver as 
begin 
    declare @start_time Datetime, @end_time Datetime, @batch_start_time Datetime,@batch_end_time Datetime
    begin try 
    set @batch_start_time = Getdate();
        PRINT '================================================';
		PRINT 'loading Silver Layer';
		PRINT '================================================';


		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables 🫡😂❤️🫡';
		PRINT '------------------------------------------------';
        set @start_time = GetDate();
        PRINT '>> Truncating Table: silver.crm_cust_info if Contain Data to Prevent Dublicate';
        truncate table silver.crm_cust_info
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        -- crm_cust_info 
        insert into  silver.crm_cust_info(
        cst_id  ,
        cst_key  ,
        cst_firstname       ,
        cst_lastname       ,
        cst_marital_status  ,
        cst_gndr            ,
        cst_create_date  )

        select
        cst_id,cst_key,
        trim(cst_firstname) as cst_firstname ,
        trim(cst_lastname) as cst_lastname ,

        case when Trim(Upper(cst_marital_status))= 'S' then 'Single'
        when Trim(Upper(cst_marital_status))= 'M' then 'Married'
        else 'N/A'
        end as cst_marital_status

        ,case  when Trim(Upper(cst_gndr))= 'M' then 'Male'
        when Trim(Upper(cst_gndr))= 'F' then 'Female'
        else 'N/A'

        end as cst_gndr

        ,cst_create_date from 
        (
        select 
        *,
        ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last

        from bronze.crm_cust_info

        ) c where  flag_last =1 

        -- test -- 
    -- select * from Silver.crm_cust_info
       PRINT '>> Data Loaded Successfully ❤️';
       set @end_time=Getdate();
       print 'load duration = '+ cast(datediff(second,@start_time,@end_time ) as nvarchar) + ' seconds';
    print '------------------------------------------------'

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info if Contain Data to Prevent Dublicate';

		truncate table silver.crm_prd_info;

		PRINT '>> Inserting Data Into: silver.crm_prd_info';  


        ------------------------------------******** crm_prd_info table *************---------------------------
        --select * from bronze.crm_prd_info

        insert into silver.crm_prd_info(
            prd_id  ,    
            cat_id   ,     
            prd_key   ,    
            prd_nm    ,     
            prd_cost     ,  
            prd_line    ,   
            prd_start_dt  , 
            prd_end_dt   
        )
        select prd_id,
        replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) as prd_cost,

        case when Upper(Trim(prd_line))= 'R' then 'Road'
        when Upper(Trim(prd_line))= 'M' then 'Mountain'
        when Upper(Trim(prd_line))= 'S' then 'Other Sales'
        when Upper(Trim(prd_line))= 'T' then 'Touring'
        else 'N/A'
        end as prd_line,
        cast(prd_start_dt as date) as prd_start_dt , cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt ) -1 as date) as prd_end_dt

        from bronze.crm_prd_info

        --test
    -- select * from silver.crm_prd_info


        PRINT '>> Data Loaded Successfully ❤️';
		SET @end_time = GETDATE();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

print '------------------------------------------------'
        ------------------------------** [crm_sales_details] table ***------ -----------------------------
        set @start_time = GetDate();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        insert into silver.crm_sales_details(
        sls_ord_num  ,
        sls_prd_key  ,
        sls_cust_id  ,
        sls_order_dt ,
        sls_ship_dt  ,
        sls_due_dt   ,
        sls_sales    ,
        sls_quantity ,
        sls_price    
        )
        select sls_ord_num,sls_prd_key,sls_cust_id,
        case when sls_order_dt = 0 or len (sls_order_dt) !=8 then null 
        else cast(cast(sls_order_dt as varchar) as date)
        end as sls_order_dt,

        case when sls_ship_dt = 0 or len (sls_ship_dt) !=8 then null 
        else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,

        case when sls_due_dt = 0 or len (sls_due_dt) !=8 then null 
        else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,

        case when sls_sales is null or sls_sales <=0 or  sls_price * sls_quantity != sls_sales  then abs(sls_price) * sls_quantity 
        else sls_sales
        end as sls_sales, sls_quantity ,

        case when sls_price is null or sls_price <=0 or  sls_sales / sls_quantity != sls_price  then sls_sales / nullif(sls_quantity,0)
        else sls_price

        end as sls_price from Bronze.crm_sales_details



        -- test

    -- select * from silver.crm_sales_details

        PRINT '>> Data Loaded Successfully ❤️';
		set @end_time = GetDate();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';
print '------------------------------------------------'







		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


        ------------------------------** [erp_cust_az12] table ***------ -----------------------------



        --select * from bronze.erp_cust_az12


        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101

		PRINT '>> Inserting Data Into: silver.erp_loc_a101';


        insert into silver.erp_cust_az12(
            cid ,   
            bdate  ,
            gen   )
        select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
        else cid end as cid,

        case when bdate > GETDATE()  then null else bdate end as bdate,
        --gen

        case when UPPER(TRIM(gen)) IN ('F','FEMALE') then 'Female'
	         when UPPER(TRIM(gen)) IN ('M','MALE') then 'Male'
	         else 'n/a' 
        END AS gen

        from bronze.erp_cust_az12

        --test

    --select * from silver.erp_cust_az12

        PRINT '>> Data Loaded Successfully ❤️';

		SET @end_time = GETDATE();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';

print '------------------------------------------------'
        ------------------------------**                                [[erp_loc_a101]] table ***------ -----------------------------


        SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		truncate  table bronze.erp_cust_az12

		

		PRINT '>> Inserting Data Into: silver.erp_cust_az12';


        insert into silver.erp_loc_a101(
         cid    ,
            cntry  
        )
        select replace(cid,'-','') as cid,case when trim(cntry) = 'DE' then 'Germany'
        when trim(cntry) in ('US','USA') then 'United States'
        when trim(cntry)= '' or cntry is null  then 'n/a' else trim(cntry)
        end as cntry     -- normalize gender values and handel unknowncases 
        from bronze.erp_loc_a101

        PRINT '>> Data Loaded Successfully ❤️';
		set @end_time = GetDate();
		print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';


        --test

    --select * from silver.erp_loc_a101

print '------------------------------------------------'

        ------------------------------**                                [[[erp_px_cat_g1v2]]] table ***------ -----------------------------
        SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';


        insert into silver.erp_px_cat_g1v2 (
        id ,          
        cat      ,
        subcat      ,
        maintenance 
        )
        select * from bronze.erp_px_cat_g1v2


        --test
    --select * from silver.erp_px_cat_g1v2

      PRINT '>> Data Loaded Successfully ❤️';

	  SET @end_time = GETDATE();
	  print 'load duration = '+ cast(DateDiff(second,@start_time,@end_time ) as nvarchar) + ' seconds';
print '------------------------------------------------'



    Set @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading silver Layer is Completed';
    PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_end_time, @batch_start_time) AS NVARCHAR) + ' seconds';
		print '-- All processes were completed successfully.'
    end try

   Begin Catch 

   print ' -----------------------------------------------'
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        print 'Error Message : ' + error_message();
        print 'Error Number : ' + cast(error_number() as Nvarchar);
		print 'Error Status : ' + cast(error_state() as Nvarchar);
		PRINT '=========================================='

   End Catch 

end;


---test


--exec silver.load_silver
