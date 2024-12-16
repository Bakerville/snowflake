CREATE DATABASE EXERCISE_DB;

USE DATABASE EXERCISE_DB;


CREATE SCHEMA MY_SCHEMA;

CREATE TABLE CUSTOMER
(
    "ID" INT,
    "first_name" String,
    "last_name" String,
    "email" String,
    "age" int,
    "city" String
)
    
Select * from CUSTOMER;


COPY INTO CUSTOMER
FROM s3://snowflake-assignments-mc/gettingstarted/customers.csv
file_format = (type=csv
            field_delimiter=','
            skip_header=1);

SELECT COUNT(*) FROM CUSTOMER
