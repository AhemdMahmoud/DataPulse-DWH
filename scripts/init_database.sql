/*

=============================================================
Create Database and Schemas
=============================================================

Script Purpose :
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.


WARNING :

	 Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.

*/

use master

go

if exists (select 1 from sys.databases where name= 'DataWarehouse')
Begin 
	alter database DataWarehouse set SINGLE_USER with ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END

GO
-- CRATE DataWarehouse AS  DATABASE 

create database DataWarehouse;
GO
-- CREATE SCHEMAS

create Schema bronze; 
go
create Schema silver;
go
create Schema gold;
go
