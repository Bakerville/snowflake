with dim_product as
(
    select *
    from {{ ref('daily_sales') }}
)
select 
    d.item_code as product_code,
    d.item_name as product_name,
    {{ current_timestamp() }} as created_at
from dim_product as d