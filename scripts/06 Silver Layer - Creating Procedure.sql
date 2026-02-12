CREATE OR REPLACE PROCEDURE silver.load_slver()
LANGUAGE plpgsql
AS
$$
DECLARE
start_time TIMESTAMP;
end_time TIMESTAMP;
duration INTERVAL;
tb1_start_time TIMESTAMP;
tb1_end_time TIMESTAMP;
tb1_duration INTERVAL;
v_state text;
v_message text;
v_detail text;
v_hint text;


BEGIN
start_time := clock_timestamp();
RAISE NOTICE '=======================';
RAISE NOTICE 'Loading Silver Layer';
RAISE NOTICE '=======================';

RAISE NOTICE '=======================';
RAISE NOTICE 'Loading CRM Table';
RAISE NOTICE '=======================';
tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;

RAISE NOTICE 'Inserting Data Into Table : silver.crm_cust_info';
INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname,
cst_material_status, cst_gndr, cst_create_date )
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
where cst_id is not null
)t
where ranking = 1 ;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.crm_cust_info :%',tb1_duration; 
RAISE NOTICE '........................................................';


tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;

RAISE NOTICE 'Inserting Data Into Table : silver.crm_prd_info';
INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost,
prd_line, prd_start_dt, prd_end_dt)
select 
prd_id,
REPLACE(LEFT(prd_key,5),'-','_') as cat_id,
SUBSTRING(prd_key from 7) as prd_key,
COALESCE(prd_cost,0) as prd_cost,
CASE WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
     WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
	 WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
	 ELSE 'n/a'
	 END as prd_line,
CAST(prd_start_dt AS DATE) as prd_start_dt,
LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.crm_prd_info :%',tb1_duration; 
RAISE NOTICE '........................................................';


tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;

RAISE NOTICE 'Inserting Data Into Table : silver.crm_sales_details';
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
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
	 
from bronze.crm_sales_details;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.crm_sales_details :%',tb1_duration; 
RAISE NOTICE '........................................................';


RAISE NOTICE '=======================';
RAISE NOTICE 'Loading ERP Table';
RAISE NOTICE '=======================';
tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;

RAISE NOTICE 'Inserting Data Into Table : silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
regexp_replace(trim(cid), '^NAS', '', 'i') as cid,
case when bdate > current_date then null
     else bdate
	 end as bdate,
case when upper(trim(gen)) = 'M' then 'Male'
     when upper(trim(gen)) = 'F' then 'Female'
	 when trim(gen) = '' or gen is null then 'n/a'
	 else gen
	 end as gen
FROM bronze.erp_cust_az12;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.erp_cust_az12 :%',tb1_duration; 
RAISE NOTICE '........................................................';



tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_loc_a101;

RAISE NOTICE 'Inserting Data Into Table : silver.erp_cust_az12';
INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
select
    replace(cid, '-', '') as cid,
    case
        when trim(cntry) in ('US', 'USA') then 'United States'
        when trim(cntry) = 'DE' then 'Germany'
        when nullif(trim(cntry), '') is null then 'n/a'
        else trim(cntry)
    end as cntry
from bronze.erp_loc_a101;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.erp_loc_a101 :%',tb1_duration; 
RAISE NOTICE '........................................................';




tb1_start_time := clock_timestamp();
RAISE NOTICE 'Truncating Table : silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

RAISE NOTICE 'Inserting Data Into Table : silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

tb1_start_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Loading Time For Table silver.erp_px_cat_g1v2:%',tb1_duration; 
RAISE NOTICE '........................................................';



end_time := clock_timestamp();
duration := end_time - start_time;
RAISE NOTICE '........................................................';
RAISE NOTICE '>>Total Loading Time :%',duration; 
RAISE NOTICE '........................................................';


EXCEPTION WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
		v_state = RETURNED_SQLSTATE,
		v_message = MESSAGE_TEXT,
		v_detail = PG_EXCEPTION_DETAIL,
		v_hint = PG_EXCEPTION_HINT;

    RAISE NOTICE 'ERROR STATE  : %', v_state;
    RAISE NOTICE 'ERROR MSG    : %', v_message;
    RAISE NOTICE 'ERROR DETAIL : %', v_detail;
    RAISE NOTICE 'ERROR HINT   : %', v_hint;

END 
$$;


CALL silver.load_slver();

select *from silver.erp_cust_az12
