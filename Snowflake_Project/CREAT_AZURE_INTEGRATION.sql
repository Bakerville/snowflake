USE DEMO_DB;

USE SCHEMA SALES_LND;

CREATE STORAGE INTEGRATION azure_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '40127cd4-45f3-49a3-b05d-315a43a9f033'
    STORAGE_ALLOWED_LOCATIONS = ('azure://snowflake2609.blob.core.windows.net/snowflakecsv');


CREATE OR REPLACE FILE FORMAT azure_fileformat
    TYPE = CSV
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1;
    

CREATE OR REPLACE STAGE stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://snowflake2609.blob.core.windows.net/snowflakecsv'
    FILE_FORMAT = azure_fileformat;

DESC STORAGE integration azure_integration;