with
    base as (
        select
            trim(id) as id_raw,
            client_id as client_id_raw,
            trim(driver_id) as driver_id_raw,
            trim(city_id) as city_id_raw,
            trim(lower(status)) as status_raw,
            trim(request_at) as request_at_raw
        from {{ source("lc", "lc262_trips") }}
    ),
    typed as (
        select
            cast(nullif(regexp_replace(id_raw, r'\D', ''), '') as int64) as id,
            client_id_raw as client_id,
            safe_cast(
                nullif(regexp_replace(driver_id_raw, r'\D', ''), '') as int64
            ) as driver_id,
            safe_cast(
                nullif(regexp_replace(city_id_raw, r'\D', ''), '') as int64
            ) as city_id,

            -- clean messy statuses
            case
                when status_raw in ('completed', 'cancelled', 'failed', 'ongoing')
                then status_raw
                when status_raw like '%cancll%'
                then 'cancelled'
                when status_raw is null or status_raw = ''
                then 'unknown'
                else status_raw
            end as status,

            -- clean date format 
            coalesce(
                safe_cast(request_at_raw as date),  -- 2025-08-01
                safe.parse_date('%m/%d/%Y', request_at_raw),  -- 08/01/2025
                safe.parse_date('%Y/%m/%d', request_at_raw),  -- 2025/08/01
                date(safe_cast(request_at_raw as timestamp))  -- ISO / timezone strings
            ) as request_at
        from base

    )
select distinct *
from typed
where typed.id is not null
