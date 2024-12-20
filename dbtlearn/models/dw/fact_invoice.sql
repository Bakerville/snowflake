{{
    config(
        materialized='incremental'
    )
}}
with DAILY_SALES as
(
    SELECT invoice_code,
        customer_code,
        item_code as product_code,
        price,
        quantity,
        invoice_date,
        current_timestamp() as created_at
    FROM {{ ref('daily_sales') }} as s
)
select * from DAILY_SALES as d
{% if is_incremental() %}
  where d.invoice_date > (select 
    CASE WHEN (select count(*) from {{ this }})=0 THEN TO_DATE('1000-01-01')
    ELSE
    MAX(invoice_date) END from {{ ref('daily_sales') }})
{% endif %}
