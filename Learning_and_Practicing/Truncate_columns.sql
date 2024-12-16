CREATE OR REPLACE STAGE s3_example2
    URL = 's3://snowflake-assignments-mc/copyoptions/example2/'

CREATE OR REPLACE FILE FORMAT my_format
    TYPE = CSV
    FIELD_DELIMITER=','
    SKIP_HEADER=1;

LIST @s3_example2;

COPY INTO CUSTOMER
    FROM @s3_example2
    file_format = (format_name = my_format)
    files = ('employees_error.csv')
    VALIDATION_MODE = RETURN_ERRORS

COPY INTO CUSTOMER
    FROM @s3_example2
    file_format = (format_name = my_format)
    files = ('employees_error.csv')
    TRUNCATECOLUMNS = TRUE 
