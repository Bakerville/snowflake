CREATE OR REPLACE STAGE s3_file_format_stage
    URL = 's3://snowflake-assignments-mc/fileformat/';


LIST @s3_file_format_stage;

CREATE OR REPLACE FILE FORMAT my_file_format
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1

DESC file format my_file_format


COPY INTO CUSTOMER
    FROM @s3_file_format_stage
    FILE_FORMAT = (format_name = my_file_format)
    pattern = ".*customer.*";

SELECT COUNT(*) FROM CUSTOMER



