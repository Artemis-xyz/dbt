{{
    config(
        materialized="view",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT
    date
    , app
    , category
    , sum(trading_volume) as trading_volume
    , sum(trading_fees) as trading_fees
    , sum(unique_traders) as unique_traders
    , sum(number_of_swaps) as number_of_swaps
    , sum(gas_cost_usd) as gas_cost_usd

    -- Standardized Metrics
    , sum(spot_dau) as spot_dau
    , sum(spot_txns) as spot_txns
    , sum(spot_volume) as spot_volume
    , sum(tvl) as tvl
    , sum(trading_fees) as trading_fees
    , sum(gross_protocol_revenue) as gross_protocol_revenue
    , sum(gas_cost_native) as gas_cost_native
    , sum(gas_cost) as gas_cost
FROM {{ ref("ez_trader_joe_metrics_by_chain") }}
GROUP BY 1, 2, 3