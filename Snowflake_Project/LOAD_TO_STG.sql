CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_STG.SP_LOAD_LND_TO_STG()
RETURNS TABLE ()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN

    CREATE OR REPLACE stream DEMO_DB.SALES_STG.stream_stg on table DEMO_DB.SALES_STG.DAILY_SALES APPEND_ONLY=TRUE;

IF ((select count(*) from  DEMO_DB.SALES_STG.DAILY_SALES)=0) THEN
    INSERT INTO DEMO_DB.SALES_STG.DAILY_SALES
    SELECT TRIM(s.id) as id,
        TRIM(s.customer_code) as customer_code,
        TRIM(s.customer_name) as customer_name,
        PARSE_JSON(s.customer_info):name::STRING as contact_person,
        PARSE_JSON(s.customer_info):email::STRING as contact_email,
        TRIM(s.customer_type_name) as customer_type_name,
        TRIM(s.area_name) as area_name,
        TRIM(s.region_name) as region_name,
        TRIM(s.invoice_number) as invoice_code,
        TRIM(s.item_code) as item_code,
        TRIM(s.item_name) as item_name,
        s.price,
        s.quantity,
        TO_DATE(s.invoice_date, 'YYYY-MM-DD') as invoice_date,
        CURRENT_TIMESTAMP() as created_at
    FROM DEMO_DB.SALES_LND.DAILY_SALES as s;
ELSE    
    INSERT INTO DEMO_DB.SALES_STG.DAILY_SALES
    SELECT TRIM(s.id) as id,
        TRIM(s.customer_code) as customer_code,
        TRIM(s.customer_name) as customer_name,
        PARSE_JSON(s.customer_info):name::STRING as contact_person,
        PARSE_JSON(s.customer_info):email::STRING as contact_email,
        TRIM(s.customer_type_name) as customer_type_name,
        TRIM(s.area_name) as area_name,
        TRIM(s.region_name) as region_name,
        TRIM(s.invoice_number) as invoice_code,
        TRIM(s.item_code) as item_code,
        TRIM(s.item_name) as item_name,
        s.price,
        s.quantity,
        TO_DATE(s.invoice_date, 'YYYY-MM-DD') as invoice_date,
        CURRENT_TIMESTAMP() as created_at
    FROM DEMO_DB.SALES_LND.stream_lnd as s
    WHERE METADATA$ACTION = 'INSERT';
END IF;
    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);

    RETURN TABLE(res);
END;

CALL DEMO_DB.SALES_STG.SP_LOAD_LND_TO_STG();

select * FROM DEMO_DB.SALES_STG.DAILY_SALES;


TRUNCATE TABLE DEMO_DB.SALES_STG.DAILY_SALES;
