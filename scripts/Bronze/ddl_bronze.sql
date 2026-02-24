/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


use DataWarehouse
go 
if  OBJECT_ID('bronze.crm_cust_info', 'U') is not null 
Drop table  bronze.crm_cust_info

go 

create table bronze.crm_cust_info(
cst_id int ,
cst_key   NVarchar(50),
cst_firstname       NVARCHAR(50),
cst_lastname        NVARCHAR(50),
cst_marital_status  NVARCHAR(50),
cst_gndr            NVARCHAR(50),
cst_create_date     DATE
);
Go 
if  OBJECT_ID('bronze.crm_prd_info', 'U') is not null 
Drop table  bronze.crm_prd_info

go
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

go 

if  OBJECT_ID('bronze.crm_sales_details', 'U') is not null 
Drop table  bronze.crm_sales_details

go

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

go 
if  OBJECT_ID('bronze.erp_loc_a101', 'U') is not null 
Drop table  bronze.erp_loc_a101

go

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO
if  OBJECT_ID('bronze.erp_cust_az12', 'U') is not null 
Drop table  bronze.erp_cust_az12

go

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

GO
if  OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is not null 
Drop table  bronze.erp_px_cat_g1v2

go

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
