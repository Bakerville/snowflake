CREATE OR REPLACE STAGE s3_stage
    URL = 's3://snowflake-assignments-mc/loadingdata/'

DESC STAGE s3_stage;

LIST @s3_stage;

COPY INTO CUSTOMER
FROM  @s3_stage
file_format = (type=csv field_delimiter=';' skip_header=1)
pattern = '.*customer.*';

SELECT * FROM CUSTOMER;

SELECT COUNT(*) FROM CUSTOMER;





