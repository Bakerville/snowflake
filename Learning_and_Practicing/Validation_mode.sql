CREATE OR REPLACE TABLE EMPLOYEES
(
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email varchar(50),
    age INT,
    department VARCHAR(50)
)

CREATE OR REPLACE STAGE copy_option_stage
    URL = 's3://snowflake-assignments-mc/copyoptions/example1';

LIST @copy_option_stage;

CREATE OR REPLACE FILE FORMAT my_format
    TYPE = 'csv'
    FIELD_DELIMITER = ','
    SKIP_HEADER =1;

COPY INTO EMPLOYEES
    FROM @copy_option_stage
    file_format = (format_name = my_format)
    files = ('/employees.csv')
    VALIDATION_MODE = RETURN_ERRORS;
    
COPY INTO EMPLOYEES
    FROM @copy_option_stage
    file_format = (format_name = my_format)
    files = ('/employees.csv')
    ON_ERROR = 'CONTINUE'


SELECT COUNT(*) FROM EMPLOYEES;


select * from table(result_scan(last_query_id()));

select * from table(validate(EMPLOYEES, job_id => '_last'));