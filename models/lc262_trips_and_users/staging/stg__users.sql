WITH base AS (
    SELECT 
        users_id AS users_id_raw,
        TRIM(LOWER(banned)) AS banned_raw,
        TRIM(LOWER(role)) AS role_raw,
        TRIM(signup_ts) AS signup_ts_raw
    FROM {{ source('lc', 'lc262_users')}}
),
typed AS (
    SELECT 
        users_id_raw AS users_id,
        CASE 
            WHEN banned_raw in ('no', 'false', '0', 'n') THEN 'no'
            WHEN banned_raw IN ('yes', 'true', '1', 'y') THEN 'yes'
            ELSE 'unknown'
        END AS banned,
        CASE WHEN role_raw IN ('client', 'driver') THEN role_raw ELSE NULL END AS role,
        COALESCE(
            SAFE_CAST(signup_ts_raw AS DATE),                     -- 2025-08-01
            SAFE.PARSE_DATE('%m/%d/%Y', signup_ts_raw),           -- 08/01/2025
            SAFE.PARSE_DATE('%Y/%m/%d', signup_ts_raw),           -- 2025/08/01
            DATE(SAFE_CAST(signup_ts_raw AS TIMESTAMP))           -- ISO / timezone strings
        ) as signup_date
        FROM base
)
SELECT * 
FROM typed WHERE users_id IS NOT NULL
