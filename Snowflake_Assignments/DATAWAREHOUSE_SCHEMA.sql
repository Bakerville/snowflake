USE DEMO_DB;

CREATE SCHEMA SALES_DW;

USE SCHEMA SALES_DW;

CREATE OR REPLACE TABLE DIM_CUSTOMER
(
    customer_code STRING,
    customer_name STRING,
    contact_person STRING,
    contact_email STRING,
    area_name STRING,
    region_name STRING
);

CREATE OR REPLACE TABLE DIM_PRODUCT
(
    product_code STRING,
    product_name STRING
);

CREATE OR REPLACE TABLE FACT_TRANSACTION_SALES
(
    invoice_code STRING,
    customer_code STRING,
    product_code STRING,
    price FLOAT,
    quantity INT,
    invoice_date DATE,
    created_at TIMESTAMP
);

CREATE OR REPLACE TABLE FACT_DAILY_TARGET
(
    date_key DATE,
    target FLOAT
);

CREATE OR REPLACE TABLE DIM_DATE
AS
    WITH GENERATE_DATE AS(
        SELECT DATE_FROM_PARTS(2019,1,1) AS DATE
        UNION ALL
        SELECT DATEADD(Day,1,DATE) as date from generate_date where date <= DATE_FROM_PARTS(2022,1,1)
    )
    select date,
        day(date) as day,
        month(date) as month,
        year(date) as year
    from generate_date;

select * from dim_date;


