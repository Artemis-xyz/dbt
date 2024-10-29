{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

SELECT
    date(to_timestamp_ntz(timestamp - 83699)) as date,
    'USD' as token,
    SUM(otc_revenue) as revenue,
    SUM(otc_revenue) as revenue_native
FROM
    {{ ref('fact_maple_otc_by_day') }} d
GROUP BY 1

UNION ALL
SELECT
    date(block_timestamp) as date,
    token,
    SUM(revenue_usd) as revenue,
    SUM(revenue_native) as revenue_native
FROM
    {{ ref('fact_maple_onchain_revenue') }}
GROUP BY 1, 2
