
select
distinct
c1.cst_gndr,
c2.gen,
case when c1.cst_gndr != 'n/a' then c1.cst_gndr
     else coalesce(c2.gen,'n/a')
	 end as nwe_gen
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid


select distinct gen from silver.erp_cust_az12

select *from silver.crm_cust_info where cst_key = 'A01Ass'



--create view table:

select *from silver.crm_prd_info
select *from silver.erp_px_cat_g1v2

create view gold.dim_customers as
select
row_number() over(order by cst_key) as customer_key,
c1.cst_id as customer_id,
c1.cst_key as customer_number,
c1.cst_firstname as first_name,
c1.cst_lastname as last_name,
c1.cst_material_status as material_status,
case when c1.cst_gndr != 'n/a' then c1.cst_gndr
     else coalesce(c2.gen,'n/a')
	 end as gender,
c1.cst_create_date as create_date,
c2.bdate as birthdate,
c3.cntry as country
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2
on c1.cst_key = c2.cid
left join silver.erp_loc_a101 c3
on c1.cst_key = c3.cid

select *from gold.dim_customers


--create view table:
drop view gold.dim_products;
create view gold.dim_products as
select
row_number() over(order by prd_start_dt,prd_key) as product_key,
p1.prd_id as product_id,
p1.prd_key as product_number,
p1.prd_nm as product_name,
p1.cat_id as category_id,
p2.cat as category,
p2.subcat as subcategory,
p2.maintenance,
p1.prd_cost as cost,
p1.prd_line as product_line,
p1.prd_start_dt as start_date,
p1.prd_end_dt as end_date
from silver.crm_prd_info p1
left join silver.erp_px_cat_g1v2 p2
on p1.cat_id = p2.id


--create a view table:
drop view gold.fact_sales;
create view gold.fact_sales as
select
s.sls_ord_num  as order_number,
p.product_key,
c.customer_key,
s.sls_order_dt as order_date,
s.sls_ship_dt as shipping_date,
s.sls_due_dt as due_date,
s.sls_sales as sales_amount,
s.sls_quantity as quantity,
s.sls_price as price
from silver.crm_sales_details s
left join gold.dim_products p
on s.sls_prd_key = p.product_number
left join gold.dim_customers c
on s.sls_cust_id = c.customer_id


select *from gold.dim_products
select *from gold.dim_customers
select *from gold.fact_sales 
select *from silver.crm_sales_details

--Foreign key integrity
select
*
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
where c.customer_key is null

select
*
from gold.fact_sales s
left join gold.dim_products p
on s.product_key = p.product_key
where p.product_key is null


select
*
from gold.dim_customers
where customer_id is null


select
*
from silver.crm_cust_info
where cst_id is null 
