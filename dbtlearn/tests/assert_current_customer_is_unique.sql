select customer_code
from {{ ref('dim_customer') }}
where current_flag = 'Y'
group by customer_code
having count(*)>1