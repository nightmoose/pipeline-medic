{{
    config(
        contract={"enforced": true}
    )
}}

select
    order_id,
    customer_id,
    order_date,
    amount,
    status
from {{ ref('stg_orders') }}
