SELECT *FROM bronze.crm_cust_info
select *from bronze.crm_prd_info
SELECT *FROM bronze.crm_sales_details
SELECT *FROM bronze.erp_cust_az12

--Table : crm_cust_info
--
select * from bronze.crm_cust_info where cst_id is null

--Check for duplicates 
select
cst_id,
count(cst_id) as count_of_cst_id
from bronze.crm_cust_info
group by cst_id
having count(cst_id) > 1

--removing duplicates
select
*
from
(
select
*,
row_number() over(partition by cst_id order by cst_create_date desc) as ranking
from bronze.crm_cust_info
where cst_id is not null
)t
where ranking = 1

--check for unwanted sapces.
select
cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

--final table:
select
cst_id,
cst_key,
trim(cst_firstname) as cst_first_name,
trim(cst_lastname) as cst_last_name,
case when upper(cst_material_status) = 'M' then 'Married'
     when upper(cst_material_status) = 'S' then 'Single'
	 else 'n/a'
	 end
	 as cst_material_status,
case when upper(cst_gndr) = 'M' then 'Male'
     when upper(cst_gndr) = 'F' then 'Female'
	 else 'n/a'
	 end
	 as cst_gndr,
cst_create_date
from 
(
select
*,
row_number() over(partition by cst_id order by cst_create_date desc) as ranking
from bronze.crm_cust_info
)t
where ranking = 1


--Table crm_prd_info


--Checking for duplicates in prd_id column
select
prd_id,
count(prd_id) as prd_id_count
from bronze.crm_prd_info
group by prd_id,prd_key
having count(prd_id) > 1

--Check for unwanted spaces.
select
prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)


--Check for nulls or negative numbers.
select
*
from bronze.crm_prd_info
where prd_cost is null or prd_cost <= 0 

--Date validation.
select
*
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt


select *from bronze.crm_sales_details
select *from bronze.crm_prd_info
select *from bronze.erp_px_cat_g1v2


select *from bronze.crm_prd_info where prd_key like '%BK-R93R-62'

--Final table:
select 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
COALESCE(prd_cost,0) as prd_cost,
CASE WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
     WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
	 WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
	 ELSE 'n/a'
	 END as prd_line,
CAST(prd_start_dt AS DATE) as prd_start_dt,
LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info

--Table : crm_sales_details
select *from bronze.crm_sales_details

select
sls_ord_num,
count(sls_ord_num) as count_of_ord_num
from bronze.crm_sales_details
group by sls_ord_num
having count(sls_ord_num) > 1

--Date validation check:
select
coalesce(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0  or sls_order_dt < 19000101 or length(sls_order_dt::text) != 8

select
*
from bronze.crm_sales_details
where sls_ship_dt > sls_due_dt

--Check for negtive, nulls and incorrect sales.
select
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales < 0 or sls_quantity < 0 or sls_price < 0 
      or sls_sales != sls_quantity*sls_price
	  or sls_sales is null or sls_quantity is null or sls_price is null




--Final Table :
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt <= 0 or length(sls_order_dt::text) != 8 then null
     else cast(cast(sls_order_dt as varchar) as date)
	 end as sls_order_dt,
	 
case when sls_ship_dt <= 0 or length(sls_ship_dt::text) != 8 then null
     else cast(cast(sls_ship_dt as varchar) as date)
	 end as sls_ship_dt,
	 
case when sls_due_dt <= 0 or length(sls_due_dt::text) != 8 then null
     else cast(cast(sls_due_dt as varchar) as date)
	 end as sls_due_dt,

case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
     then sls_quantity * abs(sls_price)
	 else sls_sales
	 end as sls_sales,

sls_quantity,

case when sls_price is null or sls_price <=0 
     then sls_sales / nullif(sls_quantity,0)
	 else sls_price
	 end as sls_price
from bronze.crm_sales_details



--Table : erp_cust_az12
select
*from bronze.erp_cust_az12

select *from bronze.crm_cust_info

--Date validation:
select
bdate
FROM bronze.erp_cust_az12
where bdate < '1900-01-01' or bdate > current_date

select
distinct(gen) as gen
FROM bronze.erp_cust_az12


--Final Table : 
SELECT
case when cid like '%NAS' then substring(cid,5,length(cid))
     else cid
	 end as cust_id,
case when bdate > current_date then null
     else bdate
	 end as bdate,
case when gen = 'M' then 'Male'
     when gen = 'F' then 'Female'
	 when gen = '' or gen is null then 'n/a'
	 else gen
	 end as gen
FROM bronze.erp_cust_az12


--Table : erp_loc_a101

select
distinct(cntry)
from
bronze.erp_loc_a101


--Final Table : 
select
replace(cid,'-','') as cid,
case when trim(cntry) in('US','USA') then 'Unites States'
     when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
	 end as cntry
from
bronze.erp_loc_a101



