select
    customer_id,
    email,
    full_name,
    region,
    cast(signup_date as date) as signup_date
from {{ ref('customers') }}
