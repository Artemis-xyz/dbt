{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics",
    )
}}


with
    price_data as ({{ get_coingecko_metrics("drift") }})
    defillama_data as ({{ get_defillama_protocol_metrics("drift") }})
SELECT 
    date,
    'drift' AS app,
    'DeFi' AS category,
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price,
    daily_avg_float_revenue as float_revenue,
    daily_avg_lending_revenue as lending_revenue,
    defillama_data.fees as perp_fees,
    defillama_data.revenue as perp_revenue,
    float_revenue + lending_revenue + defillama_data.revenue as revenue,
    defillama_data.fees as fees
FROM price_data 
LEFT JOIN {{ ref("fact_drift_prediction_markets") }} as fact_drift_prediction_markets
    ON price_data.date = fact_drift_prediction_markets.date
LEFT JOIN {{ ref("fact_drift_float_borrow_lending_revenue") }} as fact_drift_float_borrow_lending_revenue
    ON fact_drift_prediction_markets.date = fact_drift_float_borrow_lending_revenue.date
LEFT JOIN defillama_data
    ON price_data.date = defillama_data.date
