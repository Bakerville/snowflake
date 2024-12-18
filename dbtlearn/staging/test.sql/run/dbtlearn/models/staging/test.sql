
  create or replace   view demo_db.sales_lnd.test
  
   as (
    with extract_customer AS
(
    select * from demo_db.sales_dw.dim_customer
)
select * from extract_customer;
  );

