with
    base as (
        select
            users_id as users_id_raw,
            trim(lower(banned)) as banned_raw,
            trim(lower(role)) as role_raw,
            trim(signup_ts) as signup_ts_raw
        from {{ source("lc", "lc262_users") }}
    ),
    typed as (
        select
            users_id_raw as users_id,
            case
                when banned_raw in ('no', 'false', '0', 'n')
                then 'no'
                when banned_raw in ('yes', 'true', '1', 'y')
                then 'yes'
                else 'unknown'
            end as banned,
            case
                when role_raw in ('client', 'driver') then role_raw else null
            end as role,
            coalesce(
                safe_cast(signup_ts_raw as date),  -- 2025-08-01
                safe.parse_date('%m/%d/%Y', signup_ts_raw),  -- 08/01/2025
                safe.parse_date('%Y/%m/%d', signup_ts_raw),  -- 2025/08/01
                date(safe_cast(signup_ts_raw as timestamp))  -- ISO / timezone strings
            ) as signup_date
        from base
    )
select *
from typed
where users_id is not null
