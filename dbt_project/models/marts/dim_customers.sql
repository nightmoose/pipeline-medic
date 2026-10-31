select
    c.customer_id,
    c.email,
    c.full_name,
    c.region,
    c.signup_date,
    count(u.user_id) as user_account_count
from {{ ref('stg_customers') }} c
left join {{ ref('stg_users') }} u on u.customer_id = c.customer_id
group by 1, 2, 3, 4, 5
