
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

												------------------------**********Customer_DIM******************------------------------
												--explore 
/*
select * from silver.crm_cust_info
select * from silver.erp_cust_az12
select * from silver.erp_loc_a101

*/


---------*****after joining table , ckeck if any dublicates where introduced by the join logic 
-------expectation : No result\


select cst_id,count(*)  from 
(
select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry

from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid
)t group by cst_id
having  count(*) >1 


------data integration bettwen cst_gndr and gen to make one source information instead of two source  and we also must ask the expert which source is master here cst_gnder or gen  (master source of cusomer datae is CRM)
---explore

select distinct ci.cst_gndr,ca.gen
from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid
order by 1,2

--TIP
-----NuLLs often come from joined tables!
-----NuLL will appear if SQL finds no match

-- so we solve this using case when 


select distinct ci.cst_gndr,ca.gen,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr   --CRM
else coalesce(ca.gen,'n/a')
end as new_gen
from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid
order by 1,2


------now we we rename columns to frendely, meaningful names 

select 
ci.cst_id as customer_id ,
ci.cst_key  as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr   --CRM
else coalesce(ca.gen,'n/a')
end as gender,
ci.cst_create_date as create_date,
ca.bdate as birthdate,
la.cntry as country
from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid

------now the columns names become frendely, meaningful names 


-- we now sort the column into logical groups to imrove readability 

select 
ci.cst_id as customer_id ,
ci.cst_key  as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr   --CRM
else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date


from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid


---now we create surrogate key to Faster joins , stable identifiers and support SCD and avoid debendancey on source system keys
-- we can use ddl-based generation or query based using windo function (row_number) 

select 
ROW_NUMBER() over (order by ci.cst_id) as customer_key ,
ci.cst_id as customer_id ,
ci.cst_key  as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr   --CRM
else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info  ci         --Master table
left join silver.erp_cust_az12 ca         --Table A
on ci.cst_key = ca.cid
left join  silver.erp_loc_a101  la         --Table B
on ci.cst_key = la.cid



--so we ckeck test 

select distinct gender from gold.dim_customers

--------so all is perfect 





------------------------**********Prouduct_DIM******************------------------------

--explore 
/*
select * from silver.crm_prd_info
select * from  silver.erp_px_cat_g1v2

*/


---we have start date and end date if we dont need historical data we filter out all historical data so we apply  strategy if the end date is null so is is current  and we need it 



select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt

from silver.crm_prd_info pn
where prd_end_dt is null   

-------- we need to join the two table from crm and erp 


select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null   

---------we ckeck uniqueness


select prd_key,count(*)
from (
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null   ) s 
group by prd_key
having count(*)>1


--- ok evey thing is ok 

--
-- we now sort the column into logical groups to imrove readability 

select 
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.maintenance,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,



from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null   


-- ------now the columns names become frendely, meaningful names 

create view gold.dim_products as
select 
row_number ()  over (order by pn.prd_start_dt,pn.prd_key ) as product_key,
pn.prd_id as product_id ,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance ,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date

from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is null   










------------------------**********[Sales Fact]******************------------------------
---explore 
select * from silver.crm_sales_details







-- we Use the dimension's surrogate keys instead of IDs(sd.sls_prd_key,sd.sls_cust_id) to easily connect facts with dimensions

select 
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price

from silver.crm_sales_details sd
left join  gold.dim_products pr 
on sd.sls_prd_key = pr.product_key
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_key


select * from gold.dim_customers 



-- 
--
-- we now sort the column into logical groups to imrove readability  and order 

select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price

from silver.crm_sales_details sd
left join  gold.dim_products pr 
on sd.sls_prd_key = pr.product_key
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_key


select * from gold.dim_customers 

--- so now fact table become dimension keys  and dates and mesures


-------- now make the view 


create view gold.fact_sales as 
select
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price

from silver.crm_sales_details sd
left join  gold.dim_products pr 
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id






-- test if all dimension tables can successfully join to the fact atble 
--------------------------------Expected Result****************0 rows*******************
--- test for customer  dim
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key= f.customer_key
where c.customer_key is null

--It verifies that every customer_key in the fact table exists in the dimension table.


--- test for product  dim
select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key= f.customer_key
left join  gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null
