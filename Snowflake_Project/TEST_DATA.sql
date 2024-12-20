select customer_code from demo_db.sales_history.customer_history where current_flag='Y' group by customer_code having count(*)>1 ;

select product_code from demo_db.sales_history.product_history where current_flag='Y' group by product_code having count(*)>1 ;

select * from demo_db.sales_dw.dim_product;

select * from demo_db.sales_dw.fact_transaction_sales

select *
FROM demo_db.sales_history.customer_history
WHERE
   customer_code IN (select customer_code from demo_db.sales_history.customer_history group by customer_code having count(*)>2)
order by start_date;



TRUNCATE TABLE DEMO_DB.SALES_LND.DAILY_SALES;

TRUNCATE TABLE DEMO_DB.SALES_STG.DAILY_SALES;

TRUNCATE TABLE DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY;


TRUNCATE TABLE DEMO_DB.SALES_HISTORY.PRODUCT_HISTORY;


TRUNCATE TABLE DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY;

TRUNCATE TABLE DEMO_DB.SALES_DW.DIM_CUSTOMER;

TRUNCATE TABLE DEMO_DB.SALES_DW.DIM_PRODUCT;

TRUNCATE TABLE DEMO_DB.SALES_DW.FACT_TRANSACTION_SALES;