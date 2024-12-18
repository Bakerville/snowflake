with staging_table as
(
    select * from DEMO_DB.SALES_STG.DAILY_SALES
)
select * from staging_table