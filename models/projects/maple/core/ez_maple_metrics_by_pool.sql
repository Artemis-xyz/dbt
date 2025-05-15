{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        table_format="iceberg",
        database='MAPLE',
        schema='core',
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias='ez_metrics_by_pool',
        base_location_root="maple"
    )
}}

with fees as (
    SELECT
        date,
        pool_name,
        SUM(net_interest_usd) AS fees,
        SUM(platform_fees_usd) AS platform_fees,
        SUM(delegate_fees_usd) AS delegate_fees
    FROM {{ ref('fact_maple_fees') }}
    GROUP BY 1, 2
)
, tvl as (
    SELECT
        date,
        pool_name,
        -- SUM(tvl_native) AS tvl_native,
        SUM(tvl) AS tvl,
        SUM(outstanding_supply) AS outstanding_supply
    FROM {{ ref('fact_maple_agg_tvl') }}
    GROUP BY 1, 2
)
SELECT
    coalesce(fees.date, tvl.date)::TIMESTAMP_NTZ(6) AS date,
    coalesce(fees.pool_name, tvl.pool_name) as pool_name,
    fees.fees,
    fees.platform_fees,
    fees.delegate_fees,
    tvl.tvl,
    tvl.outstanding_supply
FROM fees
FULL OUTER JOIN tvl ON fees.date = tvl.date AND fees.pool_name = tvl.pool_name