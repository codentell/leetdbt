with base as (
  select 
  trim(id) as id_raw,
  client_id as client_id_raw,
  trim(driver_id) as driver_id_raw,
  trim(city_id) as city_id_raw, 
  trim(lower(status)) as status_raw, 
  trim(request_at) as request_at_raw
  from {{ source('lc','lc262_trips') }}
),
typed as (
    SELECT CAST(NULLIF(regexp_replace(id_raw, r'\D', ''), '') as int64) as id,
    client_id_raw as client_id,
    SAFE_CAST(NULLIF(regexp_replace(driver_id_raw, r'\D', ''), '') as int64) as driver_id,
    SAFE_CAST(NULLIF(regexp_replace(city_id_raw, r'\D', ''), '') as int64) as city_id,
    
    -- clean messy statuses
    CASE 
        WHEN status_raw in ('completed', 'canceled', 'failed', 'ongoing') THEN status_raw
        WHEN status_raw LIKE '%cancll%' THEN 'cancelled'
        WHEN status_raw IS NULL or status_raw = '' THEN 'unknown'
        ELSE status_raw 
    END as status,

    -- clean date format 
    COALESCE(
      SAFE_CAST(request_at_raw AS DATE),                     -- 2025-08-01
      SAFE.PARSE_DATE('%m/%d/%Y', request_at_raw),           -- 08/01/2025
      SAFE.PARSE_DATE('%Y/%m/%d', request_at_raw),           -- 2025/08/01
      DATE(SAFE_CAST(request_at_raw AS TIMESTAMP))           -- ISO / timezone strings
    ) as request_at
    FROM base

)
SELECT DISTINCT * 
FROM typed
WHERE typed.id IS NOT NULL
