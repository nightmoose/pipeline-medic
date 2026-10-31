select
    user_id,
    customer_id,
    username,
    cast(created_at as date) as created_at
from {{ ref('users') }}
