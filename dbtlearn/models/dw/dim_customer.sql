

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
            ) as record_checksum
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
                {{ ref('daily_sales') }}
        ) as t
            WHERE
            t.row_id = 1
    ),
    INSERT_DATA AS (
        SELECT
            c.customer_code,
            c.customer_name,
            c.contact_person,
            c.contact_email,
            c.area_name,
            c.region_name,
            current_timestamp() as insert_at,
            NULL as update_at,
            (CASE
                WHEN (d.primary_checksum IS NULL) THEN TO_DATE('1000-01-01')
                ELSE current_date()
            END) as start_date,
            TO_DATE('9999-12-31') as end_date,
            'Y' as current_flag,
            c.primary_checksum,
            c.record_checksum
            FROM
                LATEST_CUSTOMER_INFO as c
                LEFT JOIN DEMO_DB.DBT_DW.DIM_CUSTOMER as d ON c.primary_checksum = d.primary_checksum
            WHERE
                d.primary_checksum IS NULL
                OR (
                    c.record_checksum != d.record_checksum
                    AND d.current_flag = 'Y'
                )
        ),
    UPDATE_DATA AS (
            SELECT
                d.customer_code as customer_code,
                d.customer_name as customer_name,
                d.contact_person as contact_person,
                d.contact_email as contact_email,
                d.area_name as area_name,
                d.region_name as region_name,
                d.insert_at as insert_at,
                current_timestamp() as update_at,
                d.start_date as start_date,
                DATEADD(day,-1,current_date()) as end_date,  
                'N' as current_flag,
                d.primary_checksum,
                d.record_checksum
            FROM
                LATEST_CUSTOMER_INFO as c
                INNER JOIN DEMO_DB.DBT_DW.DIM_CUSTOMER as d ON c.primary_checksum = d.primary_checksum
            WHERE
                c.record_checksum != d.record_checksum
                AND d.current_flag = 'Y'
        ),
        KEEP_DATA AS
        (
            select d.* from DEMO_DB.DBT_DW.DIM_CUSTOMER as d LEFT JOIN LATEST_CUSTOMER_INFO as c ON d.primary_checksum=c.primary_checksum
            where d.current_flag = 'N' OR c.primary_checksum IS NULL OR (d.current_flag='Y' and d.record_checksum=c.record_checksum)
        ),
        FINAL_DATA AS
        (
            (select * from UPDATE_DATA)
            UNION ALL
            (select * from INSERT_DATA)
            UNION ALL
            (select * from KEEP_DATA)
        )
select * from FINAL_DATA