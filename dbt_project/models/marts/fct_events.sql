{{
    config(
        contract={"enforced": true}
    )
}}

select
    event_id,
    user_id,
    event_type,
    event_ts,
    order_id
from {{ ref('stg_events') }}
