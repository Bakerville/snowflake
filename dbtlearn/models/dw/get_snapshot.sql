with snapshot as
(
    select * from {{ ref('customer_snapshot') }}
)
select * from snapshot