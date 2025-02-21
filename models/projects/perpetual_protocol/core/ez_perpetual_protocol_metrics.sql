{{
    config(
        materialized="view",
        snowflake_warehouse="PERPETUAL_PROTOCOL",
        database="perpetual_protocol",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT
    date,
    app,
    category,
    sum(trading_volume) as trading_volume,
    sum(unique_traders) as unique_traders,
    sum(fees) as fees,
    sum(revenue) as revenue,
    sum(tvl) as tvl,
    sum(tvl_growth) as tvl_growth
FROM {{ ref("ez_perpetual_protocol_metrics_by_chain") }}
GROUP BY 1, 2, 3