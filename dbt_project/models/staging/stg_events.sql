select
    event_id,
    user_id,
    event_type,
    cast(event_ts as timestamp) as event_ts,
    order_id
from {{ ref('events') }}
