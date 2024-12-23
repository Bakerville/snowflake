DEMO_DB.TASK_FLOW.TS_INGEST_TO_LNDCREATE OR REPLACE PROCEDURE DEMO_DB.SALES_HISTORY.SP_LOAD_TO_CUSTOMER_HIS()
    RETURNS TABLE()
    LANGUAGE SQL
    AS
    DECLARE
        res RESULTSET;
        select_statement VARCHAR;
    BEGIN
    MERGE INTO DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY
    AS target USING (
        WITH LATEST_CUSTOMER_INFO
        AS (
            SELECT
                customer_code,
                customer_name,
                contact_person,
                contact_email,
                area_name,
                region_name,
                md5(concat_ws('||', customer_code)) as primary_checksum,
                md5(
                    concat_ws(
                        '||',
                        coalesce(customer_name, ''),
                        coalesce(contact_person, ''),
                        coalesce(contact_email, ''),
                        coalesce(area_name, ''),
                        coalesce(region_name, '')
                    )
                ) as record_checksum,
                current_timestamp() as insert_at,
                NULL as update_at,
                'Y' as current_flag
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER(
                            PARTITION BY CUSTOMER_CODE
                            ORDER BY
                                INVOICE_DATE DESC
                        ) as row_id
                    FROM
                        DEMO_DB.SALES_STG.DAILY_SALES
                ) as t
            WHERE
                t.row_id = 1
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
                LEFT JOIN DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY as d ON c.primary_checksum = d.primary_checksum
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
                INNER JOIN DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY as d ON c.primary_checksum = d.primary_checksum
            WHERE
                c.record_checksum != d.record_checksum
                AND d.current_flag = 'Y'
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
        target.contact_email,
        target.area_name,
        target.region_name,
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
        source.contact_email,
        source.area_name,
        source.region_name,
        source.insert_at,
        source.update_at,
        source.start_date,
        source.end_date,
        source.current_flag,
        source.primary_checksum,
        source.record_checksum
    );
select_statement:= 'select * from table(result_scan(last_query_id()))';
res := (EXECUTE IMMEDIATE :select_statement);
RETURN TABLE(res);
END;

--TRUNCATE TABLE DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY;

CALL DEMO_DB.SALES_HISTORY.SP_LOAD_TO_CUSTOMER_HIS();

TRUNCATE TABLE DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY;

SELECT * FROM DEMO_DB.SALES_HISTORY.CUSTOMER_HISTORY;


