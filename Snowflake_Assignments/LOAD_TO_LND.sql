CREATE OR REPLACE PROCEDURE DEMO_DB.SALES_LND.SP_LOAD_RAW_TO_LND()
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res RESULTSET;
    select_statement VARCHAR;
BEGIN
    create or replace stream DEMO_DB.SALES_LND.stream_lnd on table DEMO_DB.SALES_LND.DAILY_SALES APPEND_ONLY=TRUE;
    
    COPY INTO DAILY_SALES
    FROM @stage_azure
    FILE_FORMAT = (format_name = azure_fileformat)
    PATTERN = '.*dailysales.*';    
    --RETURN 'Successful load';
    select_statement:= 'select * from table(result_scan(last_query_id()))';
    res := (EXECUTE IMMEDIATE :select_statement);
    RETURN TABLE(res);
END;

TRUNCATE TABLE DEMO_DB.SALES_LND.DAILY_SALES;

CALL SP_LOAD_RAW_TO_LND();

SELECT * FROM DAILY_SALES ORDER BY INVOICE_DATE;






