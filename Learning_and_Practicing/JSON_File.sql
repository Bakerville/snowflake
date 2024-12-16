CREATE OR REPLACE TABLE JSON_RAW
(
    Raw Variant
);

CREATE OR REPLACE FILE FORMAT my_json
    TYPE = JSON;

CREATE OR REPLACE STAGE json_stage
    URL = 's3://snowflake-assignments-mc/unstructureddata/';

LIST @json_stage;

COPY INTO JSON_RAW
    FROM @json_stage
    file_format = (format_name = my_json)
    files = ('Jobskills.json')
    --VALIDATION_MODE = 'RETURN_ERRORS'

SELECT * FROM JSON_RAW LIMIT 1;

CREATE OR REPLACE TABLE parsed_skills
AS
    SELECT first_name, last_name, GET(skills, 0) as skill_1, GET(skills,1) as skill_2
    FROM
    (
        SELECT RAW:first_name::String as first_name,
            RAW:last_name::String as last_name,
            RAW:Skills::Array as skills
        FROM JSON_RAW
    ) as temp;

SELECT * FROM parsed_skills where first_name='Florina';

    



