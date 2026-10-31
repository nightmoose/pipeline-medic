select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status,
    c.email,
    c.full_name,
    c.region
from {{ ref('fct_orders') }} o
left join {{ ref('stg_customers') }} c on c.customer_id = o.customer_id
