{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics",
    )
}}
WITH parsed_log_metrics AS (
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
),
    price_data as ({{ get_coingecko_metrics("drift-protocol") }}),
    defillama_data as ({{ get_defillama_protocol_metrics("drift trade") }})
SELECT 
    coalesce(
        price_data.date,
        fact_drift_prediction_markets.date,
        fact_drift_float_borrow_lending_revenue.date,
        defillama_data.date,
        parsed_log_metrics.date,
        fact_drift_amm_revenue.date
    ) as date,
    'drift' AS app,
    'DeFi' AS category,
    price,
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price,
    daily_avg_float_revenue as float_revenue,
    daily_avg_lending_revenue as lending_revenue,
    parsed_log_metrics.perp_fees,
    parsed_log_metrics.perp_revenue,
    parsed_log_metrics.perp_trading_volume as trading_volume,
    parsed_log_metrics.spot_fees,
    parsed_log_metrics.spot_revenue,
    parsed_log_metrics.spot_trading_volume,
    total_revenue as excess_pnl_daily_change,
    coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0) as old_revenue,
    coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0) as revenue,
    total_revenue - (coalesce(float_revenue, 0) + 
    coalesce(lending_revenue, 0) +
    coalesce(parsed_log_metrics.perp_revenue, 0) +
    coalesce(parsed_log_metrics.spot_revenue, 0)) as amm_revenue,
    coalesce(parsed_log_metrics.perp_fees + parsed_log_metrics.spot_fees, 0) as fees,
    daily_latest_excess_pnl
FROM price_data 
LEFT JOIN {{ ref("fact_drift_amm_revenue") }} as fact_drift_amm_revenue
    ON price_data.date = fact_drift_amm_revenue.date
FULL JOIN {{ ref("fact_drift_prediction_markets") }} as fact_drift_prediction_markets
    ON price_data.date = fact_drift_prediction_markets.date
FULL JOIN {{ ref("fact_drift_float_borrow_lending_revenue") }} as fact_drift_float_borrow_lending_revenue
    ON price_data.date = fact_drift_float_borrow_lending_revenue.date
FULL JOIN defillama_data
    ON price_data.date = defillama_data.date
FULL JOIN parsed_log_metrics
    ON price_data.date = parsed_log_metrics.date
