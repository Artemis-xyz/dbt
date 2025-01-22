{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_hyperliquid_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_hyperliquid_unique_traders") }}
    ),
    daily_transactions_data as (
        select date, trades, chain
        from {{ ref("fact_hyperliquid_daily_transactions") }}
    ),
    fees_data as (
        select date, max_by(fees, date)/1e6 as fees, max_by(spot_fees, date)/1e6 as spot_fees, chain
        from {{ ref("fact_hyperliquid_fees") }}
        group by date, chain
    ),
    current_day_max AS (
        SELECT 
            date,
            chain,
            fees,
            spot_fees,
            LAG(fees) OVER (PARTITION BY chain ORDER BY date ASC) AS prev_day_fees,
            LAG(spot_fees) OVER (PARTITION BY chain ORDER BY date ASC) AS prev_day_spot_fees
        FROM fees_data
)   
select
    date,
    'hyperliquid' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders,
    trades,
    COALESCE(fees - prev_day_fees, fees) AS current_day_total_fees,
    COALESCE(spot_fees - prev_day_spot_fees, spot_fees) AS current_day_spot_fees,
    fees AS cumulative_total_fees,
    spot_fees AS cumulative_spot_fees,
from unique_traders_data
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join current_day_max using(date, chain)
where date < to_date(sysdate())
