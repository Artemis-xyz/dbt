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
    date,
    app,
    category,
    sum(tvl) as tvl,
    sum(trading_volume) as trading_volume,
    sum(trading_fees) as trading_fees,
    sum(unique_traders) as unique_traders,
    sum(txns) as txns,
    sum(gas_cost_native) as gas_cost_native
FROM {{ ref("ez_trader_joe_metrics_by_chain") }}
GROUP BY 1, 2, 3