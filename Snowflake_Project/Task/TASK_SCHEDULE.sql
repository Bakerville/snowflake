USE DEMO_DB;


CREATE SCHEMA TASK_FLOW;


CREATE OR REPLACE TASK ts_ingest_to_lnd
    SCHEDULE = '1 minute'
AS
    CALL DEMO_DB.SALES_LND.SP_LOAD_RAW_TO_LND();

    

CREATE OR REPLACE TASK ts_load_lnd_to_stg
    AFTER ts_ingest_to_lnd
AS
    CALL DEMO_DB.SALES_STG.SP_LOAD_LND_TO_STG();

    
    

CREATE OR REPLACE TASK ts_load_stg_to_customer_hist
    AFTER ts_load_lnd_to_stg
AS
    CALL DEMO_DB.SALES_HISTORY.SP_LOAD_TO_CUSTOMER_HIS();


CREATE OR REPLACE TASK ts_load_hist_to_dim_customer
    AFTER ts_load_stg_to_customer_hist
AS
    CALL DEMO_DB.SALES_DW.SP_LOAD_TO_DIM_CUSTOMER();

    
    

CREATE OR REPLACE TASK ts_load_stg_to_product_hist
    AFTER ts_load_lnd_to_stg
AS
    CALL DEMO_DB.SALES_HISTORY.SP_LOAD_TO_PRODUCT_HIS();

CREATE OR REPLACE TASK ts_load_hist_to_dim_product
    AFTER ts_load_stg_to_product_hist
AS
    CALL DEMO_DB.SALES_DW.SP_LOAD_TO_DIM_PRODUCT();

    
    
CREATE OR REPLACE TASK ts_load_stg_to_transaction_hist
    AFTER ts_load_lnd_to_stg
AS
    CALL  DEMO_DB.SALES_HISTORY.SP_LOAD_TO_TRANSACTION_HIST();
    
CREATE OR REPLACE TASK ts_load_hist_to_fact_transaction
    AFTER  ts_load_stg_to_transaction_hist
AS
    CALL DEMO_DB.SALES_DW.SP_LOAD_TO_FACT_TRANSACTION();


EXECUTE TASK ts_ingest_to_lnd;

