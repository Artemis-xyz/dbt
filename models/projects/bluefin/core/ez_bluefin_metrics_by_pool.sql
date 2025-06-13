{{
    config(
        materialized="table",
        snowflake_warehouse="BLUEFIN",
        database="bluefin",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

WITH
    spot_volumes AS(
        SELECT
            date, pool_address, SUM(amount_a_swapped_usd) AS volume_usd
        FROM {{ ref("fact_bluefin_spot_volumes") }}
        GROUP BY 1, 2
    )

    , spot_dau_txns AS (
        SELECT date, pool_address, SUM(pool_dau) AS dau, SUM(pool_txns) AS txns
        FROM {{ ref("fact_bluefin_spot_dau_txns") }}
        GROUP BY 1, 2
    )

    , spot_fees_revenue AS (
        SELECT date, pool_address, SUM(fees) AS fees, SUM(foundation_fee_allocation) AS foundation_fee_allocation, SUM(service_fee_allocation) AS service_fee_allocation
        FROM {{ ref("fact_bluefin_spot_fees_revenue") }}
        GROUP BY 1, 2
    )

    , tvl AS (
        SELECT date, pool_address, symbol_a, symbol_b, tvl
        FROM {{ ref("fact_bluefin_spot_tvl") }}
    )

    SELECT
        spot_volumes.date, 
        spot_volumes.pool_address AS pool, 
        tvl.symbol_a,
        tvl.symbol_b,
        spot_volumes.volume_usd AS spot_volume,
        spot_dau_txns.dau AS spot_dau, 
        spot_dau_txns.txns AS spot_txns, 
        spot_fees_revenue.fees AS spot_fees, 
        spot_fees_revenue.foundation_fee_allocation AS foundation_fee_allocation, 
        spot_fees_revenue.service_fee_allocation AS service_fee_allocation, 
        tvl.tvl AS spot_tvl
    FROM spot_volumes
    LEFT JOIN spot_dau_txns USING (date, pool_address)
    LEFT JOIN spot_fees_revenue USING (date, pool_address)
    LEFT JOIN tvl USING (date, pool_address)