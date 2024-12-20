--LOAD DATA TO DIM CUSTOMER
CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_DW.SP_LOAD_TO_DIM_CUSTOMER()
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN
    truncate table DEMO_DB.SALES_DW.DIM_CUSTOMER;
    
    INSERT INTO DEMO_DB.SALES_DW.DIM_CUSTOMER
    SELECT customer_code ,
        customer_name,
        contact_person,
        contact_email,
        area_name,
        region_name,
    FROM DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY
    WHERE current_flag = 'Y';
    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);
    RETURN TABLE(res);
END;

--LOAD DATA TO DIM PRODUCT
CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_DW.SP_LOAD_TO_DIM_PRODUCT()
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN
    truncate table DEMO_DB.SALES_DW.DIM_PRODUCT;
    INSERT INTO DEMO_DB.SALES_DW.DIM_PRODUCT
    SELECT product_code ,
        product_name,
    FROM DEMO_DB.SALES_HISTORY.PRODUCT_HISTORY
    WHERE current_flag = 'Y';

    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);
    RETURN TABLE(res);
END;

-- LOAD DATA TO TRANSACTIONAL FACT
CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_DW.SP_LOAD_TO_FACT_TRANSACTION()
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN

IF ((select count(*) from  DEMO_DB.SALES_DW.FACT_TRANSACTION_SALES)=0) THEN
    INSERT INTO DEMO_DB.SALES_DW.FACT_TRANSACTION_SALES
    SELECT  invoice_code,
        customer_code,
        product_code,
        price,
        quantity,
        invoice_date,
        created_at
    FROM DEMO_DB.SALES_HISTORY.TRANSACTION_HISTORY;

ELSE
    INSERT INTO DEMO_DB.SALES_DW.FACT_TRANSACTION_SALES
    SELECT  invoice_code,
        customer_code,
        product_code,
        price,
        quantity,
        invoice_date,
        created_at TIMESTAMP
    FROM DEMO_DB.SALES_HISTORY.STREAM_TRANSACTION_HIST;
END IF;
    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);
    RETURN TABLE(res);
END;

INSERT INTO FACT_DAILY_TARGET VALUES ('2021-10-01',150000000),
                            ('2021-10-02', 100000000),
                            ('2021-10-03', 90000000),
                            ('2021-10-04', 110000000)

truncate table dim_customer;
truncate table dim_product;
truncate table fact_transaction_sales;
