select * from table(result_scan(last_query_id()));

select * from table(validate(EMPLOYEES, job_id => '_last'));