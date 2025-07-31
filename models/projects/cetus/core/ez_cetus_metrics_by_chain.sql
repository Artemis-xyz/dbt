{{
    config(
        materialized="table",
        snowflake_warehouse="CETUS",
        database="cetus",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


WITH
    date_spine AS(
        SELECT
            date, 'sui' AS chain
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate())
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_cetus_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT date, 'sui' AS chain, coalesce(SUM(volume_usd), 0) AS spot_dex_volumes
        FROM {{ ref("fact_cetus_spot_volume") }}
        GROUP BY 1, 2
    )
    , spot_dau_txns AS (
        SELECT date, 'sui' AS chain, coalesce(daily_dau, 0) AS dau, coalesce(daily_txns, 0) AS txns
        FROM {{ ref("fact_cetus_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT date, 'sui' AS chain, coalesce(SUM(fees), 0) AS fees, coalesce(SUM(service_fee_allocation), 0) AS service_fee_allocation, coalesce(SUM(foundation_fee_allocation), 0) AS foundation_fee_allocation
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1, 2
    )
    , tvl AS (
        SELECT date, 'sui' AS chain, coalesce(SUM(tvl), 0) AS tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
        GROUP BY 1, 2
    )
    , market_data AS ({{ get_coingecko_metrics("cetus-protocol") }})
select
    date
    , 'cetus' as artemis_id
    , date_spine.chain 

    -- Standardized Metrics
    
    -- Usage Data
    , spot_dau_txns.dau as spot_dau
    , spot_dau_txns.dau as dau
    , spot_dau_txns.txns as spot_txns
    , spot_dau_txns.txns as txns
    , tvl.tvl as tvl
    , tvl.tvl - LAG(tvl.tvl) OVER (ORDER BY date) as tvl_net_change
    , spot_trading_volume.spot_dex_volumes as spot_volume

    -- Fee Data
    , spot_fees_revenue.fees as fees
    , spot_fees_revenue.foundation_fee_allocation as foundation_fee_allocation
    , spot_fees_revenue.service_fee_allocation as service_fee_allocation
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as updated_on
    
FROM date_spine
LEFT JOIN spot_trading_volume USING(date, chain)
LEFT JOIN spot_dau_txns USING(date, chain)
LEFT JOIN spot_fees_revenue USING(date, chain)
LEFT JOIN tvl USING(date, chain)
LEFT JOIN market_data USING(date)