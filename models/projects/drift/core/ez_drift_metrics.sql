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
    price_data as ({{ get_coingecko_metrics("drift-protocol") }}),
    defillama_data as ({{ get_defillama_protocol_metrics("drift trade") }})
SELECT 
    coalesce(price_data.date, fact_drift_prediction_markets.date, fact_drift_float_borrow_lending_revenue.date, defillama_data.date) as date,
    'drift' AS app,
    'DeFi' AS category,
    price,
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price,
    daily_avg_float_revenue as float_revenue,
    daily_avg_lending_revenue as lending_revenue,
    defillama_data.fees as perp_fees,
    defillama_data.revenue as perp_revenue,
    coalesce(float_revenue, 0) + coalesce(lending_revenue, 0) + coalesce(defillama_data.revenue,0) as revenue,
    defillama_data.fees as fees
FROM price_data 
FULL JOIN {{ ref("fact_drift_prediction_markets") }} as fact_drift_prediction_markets
    ON price_data.date = fact_drift_prediction_markets.date
FULL JOIN {{ ref("fact_drift_float_borrow_lending_revenue") }} as fact_drift_float_borrow_lending_revenue
    ON price_data.date = fact_drift_float_borrow_lending_revenue.date
FULL JOIN defillama_data
    ON price_data.date = defillama_data.date
