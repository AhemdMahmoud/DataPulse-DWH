/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


------------------------------** crm_cust_info table ***------ -----------------------------

-- ckeck for nulls or Dublication in Primary Key -----ISSUE1------------
-- Expectation : NO Result 


Select * from Bronze.crm_cust_info



Select cst_id, count(*) as counter_cst_id

from Bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is null;

--		*******************result for issue 1*******************:
/*
Select *
from Bronze.crm_cust_info
where cst_id = 29466

*/


/*
but when test this dublicate primary key , found same customer over time so i will the get the highst one 
using windo function ***Row Number*****

*/
										---------*********Solve issue1?******--------


select * from 
(
select 
*,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
from Bronze.crm_cust_info

) c where  flag_last =1 


-------------------------------------*******ISSUE 2********Check UnWanted_Spaces
---Expectation : NO Result*******-----------------------------

Select *
from Bronze.crm_cust_info
where trim(cst_firstname)!=cst_firstname or trim(cst_marital_status)!= cst_marital_status or trim(cst_lastname)!= cst_lastname or trim(cst_gndr)!= cst_gndr

--- so i found unwanted spaces in (cst_firstname,cst_lastname) columns and i will remove it go to loader to show how 

											---------*******Solve issue2*********--------
select
cst_id,cst_key,
trim(cst_firstname) as cst_firstname ,
trim(cst_lastname) as cst_lastname ,

cst_marital_status,cst_gndr,cst_create_date from Bronze.crm_cust_info




-------------------------------------*******ISSUE 3 --Data Standerdization & Consistency ********Check Consistency of vaules in  low cardinality columns ( cst_marital_status,cst_gndr) like M and S , M and F 
---Expectation : NO Result*******-----------------------------

select distinct cst_marital_status,cst_gndr from Bronze.crm_cust_info


--- i found M and S , M and F 

											---------*******Solve issue3*********--------
											--map bettwen them and mengiful name to understand --
select case when Trim(Upper(cst_marital_status))= 'S' then 'Single'
when Trim(Upper(cst_marital_status))= 'M' then 'Married'
else 'N/A'
end as cst_marital_status,
case  when Trim(Upper(cst_gndr))= 'M' then 'Male'
when Trim(Upper(cst_gndr))= 'F' then 'Female'
else 'N/A'

end as cst_gndr

from Bronze.crm_cust_info




------------------------------** [crm_prd_info] table ***------ -----------------------------

-- ckeck for nulls or Dublication in Primary Key -----ISSUE1------------
-- Expectation : NO Result 


--select * from bronze.crm_prd_info

select prd_id,count(*) as counter_prd_id from bronze.crm_prd_info
group by prd_id
having count(*)  > 1 or prd_id is null


------------------- No found any issue-------------

												-----ISSUE2------------
-- from intergraion model in docs  we need to join  crm_cust_info and crm_sales_details tables each tograter using cst_id
-- and join  crm_cust_info with  erp_px_cat_g1v2 so in   crm_cust_info i i will extract first five char to can join with erp_px_cat_g1v2 

------* explore****
select * from bronze.crm_sales_details

select * from bronze.crm_prd_info
select * from bronze.erp_px_cat_g1v2
												---------*******Solve issue2*********--------
												---i will extract first five char to can join with erp_px_cat_g1v2  using sub_string fucntion
												-- and also found difrannce in ERP (_) and CRM(-) the column which i join through it
												--so i use replace fucntion to solve this issue
select replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id
from bronze.crm_prd_info
--and test filters out unmatched data after applying transformation
where replace(SUBSTRING(prd_key,1,5),'-','_') not in (select distinct id  from bronze.erp_px_cat_g1v2)

	---------*******Solve issue3*********--------
												---i will extract first five char to can join with [sls_prd_key]  using sub_string fucntion

select SUBSTRING(prd_key,7,len(prd_key)) as prd_key
from bronze.crm_prd_info
--and test filters out unmatched data after applying transformation
where SUBSTRING(prd_key,7,len(prd_key)) not in (select distinct sls_prd_key  from bronze.crm_sales_details where )


-------------------------------------*******ISSUE 4********Check UnWanted_Spaces
---Expectation : NO Result*******-----------------------------
---select *  from bronze.crm_prd_info

select prd_nm
from bronze.crm_prd_info
where TRIM(prd_nm) != prd_nm

--- so i not found unwanted spaces  


-------------------------------------*******ISSUE 5********Check nulls or negative numbers in prd_cost
---Expectation : NO Result*******-----------------------------
---select *  from bronze.crm_prd_info

select prd_cost 
from bronze.crm_prd_info
where prd_cost is null or prd_cost<0



--- so i not found null and i wiil handel it using convert null into 0 and this also debend on bussinuss using** ISNULL function

select *,ISNULL(prd_cost, 0) as prd_cost
from bronze.crm_prd_info



-------------------------------------*******ISSUE 6 --Data Standerdization & Consistency ********Check Consistency of vaules in  low cardinality columns ( prd_line) like R and S

select distinct prd_line from bronze.crm_prd_info

											---------*******Solve issue6*********--------
											--map bettwen them and mengiful name to understand --
select distinct prd_line, case when Upper(Trim(prd_line))= 'R' then 'Road'
when Upper(Trim(prd_line))= 'M' then 'Mountain'
when Upper(Trim(prd_line))= 'S' then 'Other Sales'
when Upper(Trim(prd_line))= 'T' then 'Touring'
else 'N/A'
end as prd_line

from bronze.crm_prd_info

	--or can use it like this also 
/*
select case  Upper(Trim(prd_line))
when  'R' then 'Road'
when  'M' then 'Mountain'
when 'S' then 'Other Sales'
when 'T' then 'Touring'
else 'N/A'
end as prd_line

from bronze.crm_prd_info

*/




-------------------------------------*******ISSUE 7 -- Check for invalid Date Orders
-- explore 
select  * from bronze.crm_prd_info

select prd_start_dt,prd_end_dt from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R')

-- when explore i found prd_start_dt > prd_end_dt and this invalid  so i will handel this using lead windo function 



select cast(prd_start_dt as date),prd_end_dt , cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt ) -1 as date) as prd_end_dt

from bronze.crm_prd_info
--test after transformation
where prd_key in ('AC-HE-HL-U509-R')





------------------------------** [crm_sales_details] table ***------ -----------------------------


select * from silver.crm_cust_info
select * from silver.crm_prd_info
select *  from Bronze.crm_sales_details

--test everting in column which i join with them is good 
select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales, sls_quantity, sls_price from Bronze.crm_sales_details where sls_prd_key not in (select prd_key from silver.crm_prd_info)


select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales, sls_quantity, sls_price from Bronze.crm_sales_details where sls_cust_id not in (select cst_id from  silver.crm_cust_info)

--every thig is ok

-------------------------------------*******ISSUE 1 -- Check for invalid Date Orders 
select *  from Bronze.crm_sales_details


-- i found column like sls_order_dt and sls_ship_dt and sls_due_dt need to convert into date and len of number column dont exceeded or lower  8 and also found null and will haned all of this 
-- and also ceack for outliers by validating the boundaries of the date range 

select  nullif (sls_order_dt,0) sls_order_dt
from Bronze.crm_sales_details
where  len (sls_ordexr_dt) !=8  or sls_order_dt > 20600101 or sls_order_dt < 19000101

-- i will do also with the sls_ship_dt and sls_due_dt like the prevouse 


-------------------------------------now i will check the order date bettwen these column sls_ship_dt and sls_due,sls_order_dt  

---Expectation :*************** sls_order_dt < sls_ship_dt < sls_due *******-----------------------------


select * from Bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


---********** found every thing is ok-

-- so now will ceack (sls_sales,sls_quantity,sls_price)  negative, so i will handel this via useing case when 
--explore
select distinct  sls_cust_id, sls_sales,sls_quantity,sls_price from Bronze.crm_sales_details
where   sls_quantity*  sls_price !=  sls_sales
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0


---------------------------------------------------*******the issue solved using the next script  **
select 
case when sls_sales is null or sls_sales <=0 or  sls_price * sls_quantity != sls_sales  then abs(sls_price) * sls_quantity 
else sls_sales
end as sls_sales, sls_quantity ,

case when sls_price is null or sls_price <=0 or  sls_sales / sls_quantity != sls_price  then sls_sales / nullif(sls_quantity,0)
else sls_price

end as sls_price from Bronze.crm_sales_details

where sls_cust_id = 16470


------------------------------** [erp_cust_az12] table ***------ -----------------------------
--explore 
select * from bronze.erp_cust_az12

-- ckeck for nulls or Dublication in Primary Key -----ISSUE1------------
-- Expectation : NO Result 


select distinct cid, count(*) as counter_cid from bronze.erp_cust_az12
group by cid
having count(*) > 1


-- as we expected everything is ok 

-- from intergraion model in docs  we need to join  crm_cust_info and erp_cust_az12 tables each tograter using cst_id
Select * from Bronze.crm_cust_info
select * from bronze.erp_cust_az12
-----------**ISSUE 2****** found in erp_cust_az12 cst_id like NASAW00011000 and in erp_cust_az12 erp_cust_az12 only so we need to extract this to cant join bettwen them
Select distinct cid from Bronze.erp_cust_az12
------
select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
else cid end as cid
from bronze.erp_cust_az12


-------- test unmatched after transformation 

select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
else cid end as cid
from bronze.erp_cust_az12
where case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
else cid end  not in ( Select cst_key from Bronze.crm_cust_info)


-----------ok we not found and every thing is ok and now can be hoin bettwen them 

-- ckeck for very old customer by or future on bdate column(identify out of range date -----ISSUE3------------
-- Expectation : NO Result 


select * from bronze.erp_cust_az12
where bdate > GETDATE() or bdate < '1924-01-01'

------------------ if found and i will handel this use case when ----------

select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
else cid end as cid,

case when bdate > GETDATE() then null else bdate end as bdate

from bronze.erp_cust_az12



-----------------------now we will go into the last column in this table gen 

-- ckeck for Standerdzation and Concistincy -----ISSUE4------------
-- Expectation : NO Result 

select distinct gen
from bronze.erp_cust_az12


---------- i found issue and i will map these with meningful name  using case when 


SELECT DISTINCT
gen,
case when UPPER(TRIM(gen)) IN ('F','FEMALE') then 'Female'
	 when UPPER(TRIM(gen)) IN ('M','MALE') then 'Male'
	 else 'n/a' 
END AS gen
from bronze.erp_cust_az12 







------------------------------** [[erp_loc_a101]] table ***------ -----------------------------
--explore 
select * from bronze.erp_loc_a101



-- ckeck for nulls or Dublication in Primary Key -----ISSUE1------------
-- Expectation : NO Result 


select distinct cid, count(*) as counter_cid from bronze.erp_loc_a101
group by cid
having count(*) > 1


-----------as we expectecd every thing ok 

---************** ceack for join in both column cid and cst_key ***issue
----------explore  silver.crm_cust_info


-- select * from silver.crm_cust_info

select  cid from bronze.erp_loc_a101
where cid in (select distinct cst_key  from  silver.crm_cust_info)

---------found problem in bronze.erp_loc_a101 in - (id)  which not found this symbol in the cst_key in this table (silver.crm_cust_info)
-- iwll handel this using replace function 

select replace(cid,'-','') as cid from bronze.erp_loc_a101

-- so now every thing is ok 
select  cid from bronze.erp_loc_a101
where replace(cid,'-','') not in (select distinct cst_key  from  silver.crm_cust_info)

-----------------------*****now we will go into the last column in this table cntry *******

-- ckeck for Standerdzation and Concistincy -----ISSUE4------------
-- Expectation : NO Result 
select * from bronze.erp_loc_a101

select distinct cntry from  bronze.erp_loc_a101


--- so i found problem and now i will handel it with case when 

use DataWarehouse
select distinct cntry as old, case when Upper(trim(cntry)) = 'DE' then 'Germany'
when Upper(trim(cntry)) in ('US','USA') then 'United States'
when Upper(trim(cntry))= '' or cntry is null  then 'n/a' else trim(cntry)
end as cntry
from bronze.erp_loc_a101
order by cntry

-----------------------------**                                [[[erp_px_cat_g1v2]]] table ***------ -----------------------------
--explore


select * from bronze.erp_px_cat_g1v2

-- ckeck for nulls or Dublication in Primary Key -----ISSUE1------------
-- Expectation : NO Result 


select distinct id, count(*) as counter_id from bronze.erp_px_cat_g1v2
group by id
having count(*) > 1

-----------as we expectecd every thing ok 

---************** ceack for join in both column cat_id and id ***issue
----------explore  silver.crm_prd_info
------------- 

--select * from silver.crm_prd_info
--select * from bronze.erp_px_cat_g1v2

select  id from bronze.erp_px_cat_g1v2   --CO_PD
where id not in (select distinct cat_id   from  silver.crm_prd_info)


-------------------- check unwanted spaces 
--------expected :no result
select * from bronze.erp_px_cat_g1v2
where cat !=trim(cat) or subcat !=trim(subcat) or maintenance !=trim(maintenance)

---- as we expected  evry thing is ok 

-----------------------------------------------now we check standerdization and consistancy for low cardnality column 



select distinct cat from bronze.erp_px_cat_g1v2
select distinct subcat  from bronze.erp_px_cat_g1v2
select distinct maintenance  from bronze.erp_px_cat_g1v2


--  this all table contain data quality 
--------------------------------------------------------so now quality check for All silver tables is ok ---------------------------------------------------



