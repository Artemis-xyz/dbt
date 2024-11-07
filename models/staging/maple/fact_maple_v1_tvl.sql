{{
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH daily_orig AS (
    SELECT
        date,
        pool_name,
        asset,
        SUM(amount_funded) as daily_funded
    FROM {{ ref('fact_maple_v1_loans') }}
    GROUP BY 1,2,3
),

daily_pays AS (
    SELECT
        date,
        pool_name,
        asset,
        SUM(principal_paid) as daily_repaid
    FROM {{ ref('fact_maple_v1_repayments') }}
    WHERE principal_paid > 0
    GROUP BY 1,2,3
),

dates AS (
    SELECT 
        distinct date_trunc('day', block_timestamp) as date
    FROM {{ source('ETHEREUM_FLIPSIDE', 'fact_blocks') }}
    WHERE block_timestamp >= (SELECT MIN(date) FROM {{ ref('fact_maple_v1_loans') }})
    AND block_timestamp < DATE('2022-12-11')
),

pools AS (SELECT DISTINCT pool_name FROM {{ ref('fact_maple_v1_loans') }}),

dates_pools AS (
    SELECT
        d.date,
        p.pool_name
    FROM dates d
    CROSS JOIN pools p
),

prep_daily AS (
    SELECT
        d.date,
        d.pool_name,
        coalesce(daily_orig.asset, daily_pays.asset) as asset,
        COALESCE(daily_orig.daily_funded, 0) as daily_funded,
        COALESCE(daily_pays.daily_repaid, 0) as daily_repaid
    FROM dates_pools d
    LEFT JOIN daily_orig ON daily_orig.date = d.date AND daily_orig.pool_name = d.pool_name
    LEFT JOIN daily_pays ON daily_pays.date = d.date AND daily_pays.pool_name = d.pool_name
),

daily AS (
    SELECT
        *,
        daily_funded - daily_repaid as change
    FROM prep_daily
),

final AS (
    SELECT 
        *,
        SUM(change) OVER (PARTITION BY pool_name ORDER BY date) as outstanding
    FROM daily
)

SELECT 
    date,
    pool_name,
    asset,
    daily_funded,
    daily_repaid,
    CASE
        WHEN pool_name = 'M11 Credit WETH' OR pool_name = 'Celsius WETH Pool' 
        THEN outstanding * (SELECT price FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }} WHERE is_native = TRUE ORDER BY hour DESC LIMIT 1)
        ELSE outstanding
    END as outstanding_usd
FROM final