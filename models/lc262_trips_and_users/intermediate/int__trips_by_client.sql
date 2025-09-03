select
    request_at,
    client_id,
    count(*) as total_trips,
    sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_trips,
    sum(case when status = 'completed' then 1 else 0 end) as completed_trips
from {{ ref("stg__trips") }}
where request_at is not null and client_id is not null
group by 1, 2
