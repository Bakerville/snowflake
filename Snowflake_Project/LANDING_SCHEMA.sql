CREATE OR REPLACE DATABASE DEMO_DB;

CREATE OR REPLACE SCHEMA SALES_LND;

CREATE OR REPLACE TABLE DAILY_SALES
(
    id INT,
    customer_code STRING,
    customer_name STRING,
    customer_type_name STRING,
    area_name STRING,
    region_name STRING,
    invoice_number STRING,
    invoice_date STRING,
    item_code STRING,
    price FLOAT,
    quantity INT,
    item_name STRING,
    customer_info STRING
);

create or replace stream stream_lnd on table DEMO_DB.SALES_LND.DAILY_SALES APPEND_ONLY=TRUE;
