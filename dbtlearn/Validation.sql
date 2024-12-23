select * from demo_db.dbt_dw.dim_customer where current_flag='Y';
select * from demo_db.sales_dw.dim_customer where current_flag='Y';

select * from demo_db.dbt_dw.dim_product where current_flag='Y';
select * from demo_db.sales_dw.dim_product where current_flag='Y';

select *
FROM (select invoice_date, count(*) from demo_db.dbt_dw.fact_invoice group by invoice_date) as t1
INNER JOIN
(select invoice_date, count(*) from demo_db.sales_history.transaction_history group by invoice_date) as t2
ON  t1.invoice_date=t2.invoice_date;

--where 'Y' in (select current_flag from demo_db.dbt_dw.dim_customer as t1 where d.customer_code=t1.customer_code) AND
--'N' not in (select current_flag from demo_db.dbt_dw.dim_customer as t1 where d.customer_code=t1.customer_code);


--select * from demo_db.sales_lnd.daily_sales;

select * from demo_db.sales_history.customer_history where current_flag='Y';


