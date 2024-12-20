

WITH LATEST_PRODUCT_INFO
AS (
     SELECT item_code as product_code,
        item_name as product_name,
        md5(concat_ws('||', item_code)) as primary_checksum,
        md5(
            concat_ws(
            '||',
            coalesce(item_name, ''),
            coalesce(item_code, '')
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
                            PARTITION BY ITEM_CODE
                            ORDER BY
                                INVOICE_DATE DESC
                        ) as row_id
                    FROM
                        {{ ref('daily_sales') }} where invoice_date='2021-10-04'
                ) as t
            WHERE
                t.row_id = 1
    ),
    INSERT_DATA AS (
        SELECT
            c.product_code,
            c.product_name,
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
                LATEST_PRODUCT_INFO as c
                LEFT JOIN DEMO_DB.DBT_DW.DIM_PRODUCT as d ON c.primary_checksum = d.primary_checksum
            WHERE
                d.primary_checksum IS NULL
                OR (
                    c.record_checksum != d.record_checksum
                    AND d.current_flag = 'Y'
                )
        ),
    UPDATE_DATA AS (
            SELECT
                d.product_code as product_code,
                d.product_name as product_name,
                d.insert_at as insert_at,
                current_timestamp() as update_at,
                d.start_date as start_date,
                DATEADD(day,-1,current_date()) as end_date,  
                'N' as current_flag,
                d.primary_checksum,
                d.record_checksum
            FROM
                LATEST_PRODUCT_INFO as c
                INNER JOIN DEMO_DB.DBT_DW.DIM_PRODUCT as d ON c.primary_checksum = d.primary_checksum
            WHERE
                c.record_checksum != d.record_checksum
                AND d.current_flag = 'Y'
        ),
        FINAL_DATA AS
        (
            (select * from UPDATE_DATA)
            UNION ALL
            (select * from INSERT_DATA)
            UNION ALL
            (
            select d.* from DEMO_DB.DBT_DW.DIM_PRODUCT as d LEFT JOIN LATEST_PRODUCT_INFO as c ON d.primary_checksum=c.primary_checksum
            where d.current_flag = 'N' OR c.primary_checksum IS NULL OR (d.current_flag='Y' and d.record_checksum=c.record_checksum)
            )
        )
SELECT * FROM FINAL_DATA