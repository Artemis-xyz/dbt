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
        SELECT date, pool_address, SUM(fees) AS fees, SUM(service_fee_allocation) AS service_fee_allocation, SUM(foundation_fee_allocation) AS foundation_fee_allocation
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1, 2
    )

    , tvl AS (
        SELECT date, pool_address, symbol_a, symbol_b, tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
    )

    SELECT
        spot_volumes.date
        , 'cetus' as artemis_id
        , spot_volumes.pool_address as pool
        , tvl.symbol_a
        , tvl.symbol_b

        -- Standardized Metrics

        -- Usage Data
        , spot_dau_txns.dau as spot_dau
        , spot_dau_txns.dau as dau
        , spot_dau_txns.txns as spot_txns
        , spot_dau_txns.txns as txns
        , tvl.tvl as spot_tvl
        , spot_volumes.volume_usd as spot_volume

        -- Fee Data
        , spot_fees_revenue.fees as spot_fees
        , spot_fees_revenue.service_fee_allocation as service_fee_allocation
        , spot_fees_revenue.foundation_fee_allocation as foundation_fee_allocation
        
    FROM spot_volumes
    LEFT JOIN spot_dau_txns USING (date, pool_address)
    LEFT JOIN spot_fees_revenue USING (date, pool_address)
    LEFT JOIN tvl USING (date, pool_address)