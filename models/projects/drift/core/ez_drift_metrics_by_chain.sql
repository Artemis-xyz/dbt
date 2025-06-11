{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
WITH drift_data AS (
    SELECT 
        block_date AS date,
        SUM(IFF(market_type = 1, total_taker_fee, 0)) AS perp_fees,
        SUM(IFF(market_type = 1, total_revenue, 0)) AS perp_revenue,
        SUM(IFF(market_type = 1, total_volume, 0)) AS perp_trading_volume,
        SUM(IFF(market_type = 0, total_revenue, 0)) AS spot_fees,
        SUM(IFF(market_type = 0, total_taker_fee, 0)) AS spot_revenue,
        SUM(IFF(market_type = 0, total_volume, 0)) AS spot_trading_volume
    FROM {{ ref("fact_drift_parsed_logs") }}
    GROUP BY
        block_date
)
select
    date
    , 'drift' as app
    , 'DeFi' as category
    , 'solana' as chain
    , perp_trading_volume as trading_volume

    -- Standardized metrics
    , trading_volume as perp_volume
    , spot_trading_volume as spot_volume
    
    -- Cashflow metrics
    , perp_fees
    , spot_fees
    , coalesce(perp_fees, 0) + coalesce(spot_fees, 0) as ecosystem_revenue
from drift_data
where date < to_date(sysdate())