{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT
    date,
    sum(trading_volume) as trading_volume,
    sum(unique_traders) as unique_traders,
    sum(txns) as txns,
    sum(fees) as fees,
    sum(spot_fees) as spot_fees,
    sum(perp_fees) as perp_fees,
    sum(auction_fees) as auction_fees,
    sum(daily_burn) as daily_burn,
    avg(price) as price,
    -- protocol’s revenue split between HLP (supplier) and AF (holder) at a ratio of 46%:54%
    sum(primary_supply_side_revenue) as primary_supply_side_revenue,
    -- add daily burn back to the revenue
    sum(revenue) as revenue
FROM {{ ref("ez_hyperliquid_metrics_by_chain") }}
GROUP BY 1