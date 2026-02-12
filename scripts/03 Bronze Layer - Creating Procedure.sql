
CREATE OR REPLACE PROCEDURE bronze_load_bronze()
LANGUAGE plpgsql
AS 
$$
DECLARE 
start_time TIMESTAMP;
end_time TIMESTAMP;
tb1_start_time TIMESTAMP;
tb1_end_time TIMESTAMP;
duration INTERVAL;
tb1_duration INTERVAL;
v_state text;
v_message text;
v_detail text;
v_hint text;

BEGIN
start_time :=clock_timestamp();
RAISE NOTICE '=======================';
RAISE NOTICE 'Loading Bronze Layer';
RAISE NOTICE '=======================';

RAISE NOTICE '=======================';
RAISE NOTICE 'Loading CRM Table';
RAISE NOTICE '=======================';

 -- 01 - bronze_crm_cust_info
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.crm_cust_info';

TRUNCATE TABLE bronze.crm_cust_info;

RAISE NOTICE '>>Inserting Data Into Table : bronze.crm_cust_info';

COPY bronze.crm_cust_info
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/cust_info.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.crm_cust_info :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';


-- 02 - bronze_crm_prd_info
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.crm_prd_info';

TRUNCATE TABLE bronze.crm_prd_info;

RAISE NOTICE '>>Inserting Data Into Table : bronze.crm_prd_info';

COPY bronze.crm_prd_info
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/prd_info.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.prd_cust_info :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';

-- 03 - bronze_crm_sales_details
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.crm_sales_details';

TRUNCATE TABLE bronze.crm_sales_details;

RAISE NOTICE '>>Inserting Data Into Table : bronze.crm_sales_details';

COPY bronze.crm_sales_details
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/sales_details.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.crm_sales_details :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';


-- 04 - bronze_erp_cust_az12
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.erp_cust_az12';

TRUNCATE TABLE bronze.erp_cust_az12;

RAISE NOTICE '>>Inserting Data Into Table : bronze.erp_cust_az12';

COPY bronze.erp_cust_az12
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/CUST_AZ12.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.erp_cust_az12 :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';


-- 05 - bronze_erp_loc_a101
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.erp_loc_a101';

TRUNCATE TABLE bronze.erp_loc_a101;

RAISE NOTICE '>>Inserting Data Into Table : bronze.erp_loc_a101';

COPY bronze.erp_loc_a101
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/LOC_A101.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.erp_loc_a101 :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';


-- 06 - bronze_erp_px_cat_g1v2
tb1_start_time := clock_timestamp();
RAISE NOTICE '>>Truncating Table : bronze.erp_px_cat_g1v2';

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

RAISE NOTICE '>>Inserting Data Into Table : bronze.erp_px_cat_g1v2';

COPY bronze.erp_px_cat_g1v2
FROM 'C:/Program Files/PostgreSQL/18/data/data - data warehouse/PX_CAT_G1V2.csv'
CSV HEADER;

tb1_end_time:= clock_timestamp();
tb1_duration:= tb1_end_time - tb1_start_time;

RAISE NOTICE '--------------------------------------------------------';
RAISE NOTICE '>>Loading Time For Table bronze.erp_px_cat_g1v2 :%',tb1_duration; 
RAISE NOTICE '--------------------------------------------------------';


end_time :=clock_timestamp();
duration:=end_time - start_time;

RAISE NOTICE 'Total Loading Time : %', duration;

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

CALL bronze_load_bronze()


