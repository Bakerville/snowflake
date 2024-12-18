with dim_customer as
(
    select *
    from {{ ref('daily_sales') }}
)
select 
    c.customer_code,
    c.customer_name,
    c.contact_person,
    c.contact_email,
    c.area_name,
    c.region_name,
    {{ current_timestamp() }} as created_at
from dim_customer as c