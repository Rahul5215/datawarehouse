/*
Here’s a professional GitHub explanation paragraph for this Bronze table creation script 👇

🥉 Bronze Layer – Source System Table Structures

This script defines the raw staging tables for multiple source systems within the Bronze layer of the data warehouse. The tables are structured to closely mirror the original CRM and ERP source data without applying any transformations or business rules.

==> CRM Source Tables :-

> crm_cust_info – Customer master information including demographic and status attributes

> crm_prd_info – Product reference data with lifecycle dates

> crm_sales_details – Transactional sales records including order, shipping, pricing, and quantity details

==> ERP Source Tables :-

> erp_loc_a101 – Customer location mapping data

> erp_cust_az12 – Additional customer demographic details

> erp_px_cat_g1v2 – Product category and maintenance classification data

Each table is created using a DROP TABLE IF EXISTS pattern to allow repeatable execution during development. The Bronze layer preserves source data in its original structure, ensuring traceability and serving as the foundation for downstream cleansing and transformation in the Silver layer.
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt TIMESTAMP,
prd_end_dt TIMESTAMP
);


DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50)
);


