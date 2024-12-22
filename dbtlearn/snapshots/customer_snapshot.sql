{% snapshot customer_snapshot %}
{{
    config(
        target_schema = 'customer_snapshot',
        unique_key = 'customer_code',
        strategy = 'check',
        check_cols = ["customer_name", "contact_person", "contact_email", "area_name","region_name"],
        data_valid_to_current = "dateadd(day, -1, current_date())"
    )
}}
select customer_code,
customer_name,
contact_person,
contact_email,
region_name,
area_name
from 
(select *, row_number() over(partition by customer_code order by invoice_date DESC) as row_id from {{ ref('daily_sales') }})
where row_id = 1
{% endsnapshot %}