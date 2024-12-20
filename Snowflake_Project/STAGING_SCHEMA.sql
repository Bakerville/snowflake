USE DEMO_DB;

CREATE OR REPLACE SCHEMA SALES_STG;

USE SCHEMA SALES_STG;

CREATE OR REPLACE TABLE DAILY_SALES
(
    id INT,
    customer_code STRING,
    customer_name STRING,
    contact_person STRING,
    contact_email STRING,
    customer_type_name STRING,
    area_name STRING,
    region_name STRING,
    invoice_code STRING,
    item_code STRING,
    item_name STRING,
    price FLOAT,
    quantity INT,
    invoice_date DATE,
    created_at DATETIME
)