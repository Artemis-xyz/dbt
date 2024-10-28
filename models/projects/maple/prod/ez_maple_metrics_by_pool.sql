{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics_by_pool'
    )
}}

with fees as (
    SELECT
        date,
        pool_name,
        asset,
        SUM(fees_native) AS fees_native,
        SUM(fees) AS fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1, 2, 3
)
, tvl as (
    SELECT
        date,
        pool_name,
        asset,
        SUM(tvl_native) AS tvl_native,
        SUM(tvl) AS tvl
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1, 2, 3
)
SELECT
    coalesce(fees.date, tvl.date) as date,
    coalesce(fees.pool_name, tvl.pool_name) as pool_name,
    fees.asset,
    fees.fees_native,
    fees.fees,
    tvl.tvl_native,
    tvl.tvl
FROM fees
FULL OUTER JOIN tvl ON fees.date = tvl.date AND fees.pool_name = tvl.pool_name AND fees.asset = tvl.asset