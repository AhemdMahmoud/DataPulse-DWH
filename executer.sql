
use DataWarehouse
go 
exec bronze.load_bronze
go
exec silver.load_silver
go 
EXEC gold.sp_refresh_views;