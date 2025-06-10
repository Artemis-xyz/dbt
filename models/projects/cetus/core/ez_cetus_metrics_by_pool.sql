{{
    config(
        materialized="table",
        snowflake_warehouse="CETUS",
        database="cetus",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

WITH
    spot_volumes AS(
        SELECT
            date, pool_address, SUM(amount_a_swapped_usd) AS volume_usd
        FROM {{ ref("fact_cetus_spot_volume") }}
        GROUP BY 1, 2
    )

    , spot_dau_txns AS (
        SELECT date, pool_address, SUM(pool_dau) AS dau, SUM(pool_txns) AS txns
        FROM {{ ref("fact_cetus_spot_dau_txns") }}
        GROUP BY 1, 2
    )

    , spot_fees_revenue AS (
        SELECT date, pool_address, SUM(fees) AS fees, SUM(service_cash_flow) AS service_cash_flow, SUM(foundation_cash_flow) AS foundation_cash_flow
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1, 2
    )

    , tvl AS (
        SELECT date, pool_address, symbol_a, symbol_b, tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
    )

    SELECT DISTINCT
        spot_volumes.date, 
        spot_volumes.pool_address AS pool, 
        tvl.symbol_a,
        tvl.symbol_b,
        spot_volumes.volume_usd AS spot_volume,
        spot_dau_txns.dau AS spot_dau, 
        spot_dau_txns.txns AS spot_txns, 
        spot_fees_revenue.fees AS spot_fees, 
        spot_fees_revenue.service_cash_flow AS spot_service_cash_flow, 
        spot_fees_revenue.foundation_cash_flow AS spot_foundation_cash_flow, 
        tvl.tvl AS spot_tvl
    FROM spot_volumes
    LEFT JOIN spot_dau_txns USING (date, pool_address)
    LEFT JOIN spot_fees_revenue USING (date, pool_address)
    LEFT JOIN tvl USING (date, pool_address)