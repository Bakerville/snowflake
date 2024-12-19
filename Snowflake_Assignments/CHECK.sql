select 
(select count(*) from DEMO_DB.SALES_DW.FACT_TRANSACTION_SALES)=(select count(*) from DEMO_DB.SALES_LND.DAILY_SALES) as check_number_transaction,
(select CASE WHEN array_agg(customer_code)=[] THEN 'Legit'
        ELSE ARRAY_TO_STRING(array_agg(customer_code), '-') END AS check_customer_history
from(
    select customer_code
    from DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY
    where current_flag='Y'
    group by customer_code having count(*)>1
) as temp) as check_customer_history,
(select CASE WHEN array_agg(product_code)=[] THEN 'Legit'
        ELSE ARRAY_TO_STRING(array_agg(product_code), '-') END AS check_product_history
from(
    select product_code
    from DEMO_DB.SALES_HISTORY.PRODUCT_HISTORY
    where current_flag='Y'
    group by product_code having count(*)>1
) as temp) as check_product_history



