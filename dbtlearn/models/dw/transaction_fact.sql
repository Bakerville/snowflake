with transaction_fact as
(
    select *
    from {{ ref('daily_sales') }}
)
select 
    invoice_code,
    customer_code,
    item_code as product_code,
    price,
    quantity,
    invoice_date,
    {{ current_timestamp() }} as created_at
from transaction_fact