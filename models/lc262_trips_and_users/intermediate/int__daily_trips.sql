select
    request_at,
    city_id,
    count(*) as total_trips,
    sum(case when status = 'cancelled' then 1 else 0 end) cancelled_trips
from {{ ref("stg__trips") }}
where request_at is not null and city_id is not null
group by 1, 2
