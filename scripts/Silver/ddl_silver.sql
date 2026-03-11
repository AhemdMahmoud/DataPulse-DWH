/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'Silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'Silver' Tables
===============================================================================
*/


use DataWarehouse
go 
if  OBJECT_ID('silver.crm_cust_info', 'U') is not null 
Drop table  silver.crm_cust_info

go 


create table silver.crm_cust_info(
cst_id int ,
cst_key   NVarchar(50),
cst_firstname       NVARCHAR(50),
cst_lastname        NVARCHAR(50),
cst_marital_status  NVARCHAR(50),
cst_gndr            NVARCHAR(50),
cst_create_date     DATE,
dwh_create_date      Datetime2 DEFAULT Getdate()
);
Go 
if  OBJECT_ID('silver.crm_prd_info', 'U') is not null 
Drop table  silver.crm_prd_info

go
CREATE TABLE silver.crm_prd_info (
    prd_id       int,
    cat_id      NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_create_date      Datetime2 DEFAULT Getdate()
);

go 

if  OBJECT_ID('silver.crm_sales_details', 'U') is not null 
Drop table  silver.crm_sales_details

go

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt Date,
    sls_ship_dt  Date,
    sls_due_dt   Date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date      Datetime2 DEFAULT Getdate()
);

go 
if  OBJECT_ID('silver.erp_loc_a101', 'U') is not null 
Drop table  silver.erp_loc_a101

go

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_create_date      Datetime2 DEFAULT Getdate()
);
GO
if  OBJECT_ID('silver.erp_cust_az12', 'U') is not null 
Drop table  silver.erp_cust_az12

go

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date      Datetime2 DEFAULT Getdate()
);

GO
if  OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is not null 
Drop table  silver.erp_px_cat_g1v2

go

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date      Datetime2 DEFAULT Getdate()
);
