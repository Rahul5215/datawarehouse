🏗 End-to-End Data Warehouse Project (PostgreSQL)
📌 Project Overview

This project demonstrates the design and implementation of a complete layered data warehouse using PostgreSQL, following the Medallion Architecture (Bronze → Silver → Gold) pattern.

The warehouse integrates data from multiple source systems (CRM and ERP), performs structured transformations, and builds a dimensional star schema to support advanced business analytics and KPI reporting.

The goal of this project is to simulate a real-world enterprise data warehouse pipeline — from raw ingestion to business-ready insights.

🏛 Architecture Design

The warehouse is structured into three layers:

🥉 Bronze Layer – Raw Ingestion

• Stores source data exactly as received

• CRM and ERP systems loaded via COPY

• No transformations applied

• Ensures traceability and auditability

🥈 Silver Layer – Cleansing & Standardization

• Data type enforcement

• Deduplication using window functions

• Null handling and domain standardization

• Multi-source integration (CRM + ERP)

• Audit columns added (dwh_create_date)

🥇 Gold Layer – Analytical Star Schema

Dimension Views:

- dim_customers

- dim_products

Fact View:

- fact_sales

- Surrogate key generation

- Clean fact-to-dimension relationships

- Optimized for analytical queries

🗂 Data Model (Star Schema)

The Gold layer follows a star schema:

> Fact Table

fact_sales → transactional sales metrics

> Dimension Tables

dim_customers → customer master + demographics + location

dim_products → product attributes + category classification

This design enables efficient aggregation and KPI reporting.

⚙ ETL Implementation

The project includes stored procedures to automate:

- Bronze data ingestion

- Silver transformation

- Gold layer modeling

- Execution time tracking

- Error handling with diagnostics

- Bulk loading is implemented using PostgreSQL COPY for performance.

📊 Business KPIs & Analytics

The project includes advanced SQL analytics built on the Gold layer:

📈 Growth Metrics

- Year-over-Year (YoY) Growth

- Month-over-Month (MoM) Growth

- Order growth rate

💰 Financial Metrics

- Average Order Value (AOV)

- Profit & Profit Margin

- Country-wise profitability

- Revenue contribution %

🛒 Product Analytics

- Top 10 performing products

- Category & subcategory distribution

- Quantity-based ranking

- Revenue contribution of top products

👥 Customer Analytics

- Customer Lifetime Value (CLV)

- Retention Rate

- Repeat Purchase Rate

Age-group-based segmentation
