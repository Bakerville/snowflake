USE DEMO_DB;

CREATE OR REPLACE TABLE SCD_TABLE
(
    customer_code STRING,
    customer_name STRING,
    contact_person STRING,
    primary_checksum STRING,
    record_checksum STRING,
    insert_at DATETIME,
    update_at DATETIME,
    start_date DATE,
    end_date DATE,
    current_flag STRING
);
truncate table scd_table;

INSERT INTO SCD_TABLE VALUES ('1', 'Messi', 'messuy@email.com','1', '1', '2024-12-01','2024-12-01' ,'1000-01-01','2024-12-01', 'N'),
                            ('2', 'Ronaldo', 'cr7@email.com','2', '2', '2024-01-01', null,'1000-01-01','9999-12-31', 'Y'),
                            ('3', 'Fermat', 'fermat@email.com','3', '4', '2024-12-02', null,'1000-01-01','9999-12-31', 'Y'),
                            ('1', 'Messi', 'messiu@email.com','1', '3', '2024-12-02', null,'1000-01-02','9999-12-31', 'Y')
select * from scd_table order by primary_checksum;
                            
CREATE TABLE INPUT_TABLE
(
    customer_code STRING,
    customer_name STRING,
    contact_person STRING,
    primary_checksum STRING,
    record_checksum STRING
);
truncate table INPUT_TABLE;
INSERT INTO INPUT_TABLE VALUES ('3', 'Aris', 'ace@email.com','3', '17'),
                            ('2', 'Ronaldo', 'cris7@email.com','2', '13')
                            
select * from INPUT_TABLE;

                            
MERGE INTO SCD_TABLE AS target USING (
    WITH LATEST_CUSTOMER_INFO AS (
        SELECT
            customer_code,
            customer_name,
            contact_person,
            primary_checksum,
            record_checksum,
            current_timestamp() as insert_at,
            NULL as update_at,
            'Y' as current_flag
        FROM
            INPUT_TABLE
    ),
    INSERT_DATA AS (
        SELECT
            c.*,
            CASE
                WHEN d.primary_checksum is null THEN TO_DATE('1000-01-01')
                ELSE current_date()
            END as start_date,
            TO_DATE('9999-12-31') as end_date,
            'I' as insert_or_update
        FROM
            LATEST_CUSTOMER_INFO as c
            LEFT JOIN SCD_TABLE as d ON c.primary_checksum = d.primary_checksum
        WHERE
            d.primary_checksum IS NULL
            OR (
                c.record_checksum != d.record_checksum
                AND d.current_flag = 'Y'
            )
    ),
    UPDATE_DATA AS (
        SELECT
            c.*,
            TO_DATE('1000-01-01') as start_date,
            TO_DATE('9999-12-31') as end_date,
            'U' as insert_or_update
        FROM
            LATEST_CUSTOMER_INFO as c
            INNER JOIN SCD_TABLE as d ON c.primary_checksum = d.primary_checksum
        WHERE
            c.record_checksum != d.record_checksum and d.current_flag='Y'
    ),
    FINAL_DATA AS (
        (
            SELECT
                *
            FROM
                INSERT_DATA
        )
        UNION ALL
            (
                SELECT
                    *
                FROM
                    UPDATE_DATA
            )
    )
    SELECT
        *
    FROM
        FINAL_DATA
) as source ON target.primary_checksum = source.primary_checksum
AND source.insert_or_update = 'U'
AND target.current_flag = 'Y'
WHEN MATCHED THEN
UPDATE
SET
    target.update_at = current_timestamp(),
    target.end_date = DATEADD(day, -1, current_date()),
    target.current_flag = 'N'
WHEN NOT MATCHED THEN
INSERT
    (
        target.customer_code,
        target.customer_name,
        target.contact_person,
        target.insert_at,
        target.update_at,
        target.start_date,
        target.end_date,
        target.current_flag,
        target.primary_checksum,
        target.record_checksum
    )
VALUES
    (
        source.customer_code,
        source.customer_name,
        source.contact_person,
        source.insert_at,
        source.update_at,
        source.start_date,
        source.end_date,
        source.current_flag,
        source.primary_checksum,
        source.record_checksum
    )
