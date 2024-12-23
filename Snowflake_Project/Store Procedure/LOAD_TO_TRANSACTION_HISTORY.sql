CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_HISTORY.SP_LOAD_TO_TRANSACTION_HIST()
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN
    CREATE OR REPLACE stream DEMO_DB.SALES_HISTORY.stream_transaction_hist on table DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY APPEND_ONLY=TRUE;

IF ((select count(*) from  DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY)=0) THEN
    INSERT INTO DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY
    SELECT invoice_code,
        customer_code,
        item_code as product_code,
        price,
        quantity,
        invoice_date,
        current_timestamp() as created_at
    FROM DEMO_DB.SALES_STG.DAILY_SALES;
ELSE
    INSERT INTO DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY
    SELECT invoice_code,
        customer_code,
        item_code as product_code,
        price,
        quantity,
        invoice_date,
        current_timestamp() as created_at
    FROM DEMO_DB.SALES_STG.stream_stg as s
    WHERE METADATA$ACTION = 'INSERT';
END IF;

    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);

    RETURN TABLE(res);
END;

select * from DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY;



