{{
    config(
        materialized='incremental'
    )
}}
with DAILY_SALES as
(
    SELECT s.id as id,
        TRIM(s.customer_code) as customer_code,
        TRIM(s.customer_name) as customer_name,
        PARSE_JSON(s.customer_info):name::STRING as contact_person,
        PARSE_JSON(s.customer_info):email::STRING as contact_email,
        TRIM(s.customer_type_name) as customer_type_name,
        TRIM(s.area_name) as area_name,
        TRIM(s.region_name) as region_name,
        TRIM(s.invoice_number) as invoice_code,
        TRIM(s.item_code) as item_code,
        TRIM(s.item_name) as item_name,
        s.price,
        s.quantity,
        TO_DATE(s.invoice_date, 'YYYY-MM-DD') as invoice_date,
        CURRENT_TIMESTAMP() as created_at
    FROM DEMO_DB.SALES_LND.DAILY_SALES as s
)
select * from DAILY_SALES as d
{% if is_incremental() %}
  where d.invoice_date > (select 
    CASE WHEN (select count(*) from {{ this }})=0 THEN TO_DATE('1000-01-01')
    ELSE
    MAX(invoice_date) END from DEMO_DB.DBT_STG.DAILY_SALES)
{% endif %}
