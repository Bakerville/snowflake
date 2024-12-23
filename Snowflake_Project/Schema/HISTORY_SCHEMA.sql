USE DEMO_DB;

CREATE SCHEMA SALES_HISTORY;

USE SCHEMA SALES_HISTORY;

CREATE OR REPLACE TABLE CUSTOMER_HISTORY
(
    customer_code STRING,
    customer_name STRING,
    contact_person STRING,
    contact_email STRING,
    area_name STRING,
    region_name STRING,
    insert_at TIMESTAMP,
    update_at TIMESTAMP,
    start_date DATE,
    end_date DATE,
    current_flag STRING,
    primary_checksum STRING,
    record_checksum STRING
);
ALTER TABLE CUSTOMER_HISTORY CLUSTER BY (current_flag);

CREATE OR REPLACE TABLE PRODUCT_HISTORY
(
    product_code STRING,
    product_name STRING,
    insert_at TIMESTAMP,
    update_at TIMESTAMP,
    start_date DATE,
    end_date DATE,
    current_flag STRING,
    primary_checksum STRING,
    record_checksum STRING
);
ALTER TABLE PRODUCT_HISTORY CLUSTER BY (current_flag);

CREATE OR REPLACE TABLE TRANSACTION_HISTORY
(
    invoice_code STRING,
    customer_code STRING,
    product_code STRING,
    price FLOAT,
    quantity INT,
    invoice_date DATE,
    created_at TIMESTAMP
);

