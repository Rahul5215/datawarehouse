/*
🏗 Schema Initialization (Medallion Architecture Setup)

This script initializes the foundational schema structure of the data warehouse by creating the Bronze, Silver, and Gold layers following the Medallion architecture pattern.

Bronze – Raw ingestion layer storing source data as-is

Silver – Cleansed and standardized transformation layer

Gold – Business-ready analytical layer optimized for reporting

The use of CREATE SCHEMA IF NOT EXISTS ensures idempotent execution, allowing the script to run safely multiple times without causing errors. This establishes a clear separation of responsibilities across data processing stages and enforces modular, scalable warehouse design.
*/

CREATE SCHEMA IF NOT EXISTS  bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
