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
        SELECT 
        date(timestamp) AS date, 
        chain,
        max_by(fees, timestamp) / 1e6 AS total_fees,
        max_by(spot_fees, timestamp) / 1e6 AS cumulative_spot_fees,
        CASE 
            WHEN date(timestamp) >= '2024-12-23' THEN 
                total_fees - COALESCE(LAG(total_fees) OVER (PARTITION BY chain ORDER BY date ASC), 0)
            ELSE NULL
        END AS trading_fees,
        CASE 
            WHEN date(timestamp) >= '2024-12-23' THEN 
                cumulative_spot_fees - COALESCE(LAG(cumulative_spot_fees) OVER (PARTITION BY chain ORDER BY date ASC), 0)
            ELSE NULL
        END AS spot_fees,
        COALESCE(total_fees - COALESCE(LAG(total_fees) OVER (PARTITION BY chain ORDER BY date ASC), 0), 0) 
        - COALESCE(cumulative_spot_fees - COALESCE(LAG(cumulative_spot_fees) OVER (PARTITION BY chain ORDER BY date ASC), 0), 0
        ) AS perp_fees
    FROM {{ ref("fact_hyperliquid_fees") }}
    group by date(timestamp), chain
    ),
    auction_fees_data as (
        select date, auction_fees, chain
        from {{ ref("fact_hyperliquid_auction_fees") }}
    ),
    daily_burn_data as (
        select date, daily_burn, chain
        from {{ ref("fact_hyperliquid_daily_burn") }}
    ),
    daily_price_data as (
        select date, shifted_token_price_usd as daily_price, coingecko_id as chain 
        from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
        where coingecko_id = 'hyperliquid'
    )
select
    date,
    'hyperliquid' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders,
    trades, 
    COALESCE(trading_fees, 0) + COALESCE(auction_fees, 0) AS fees,
    COALESCE(spot_fees, 0) AS spot_fees,
    COALESCE(perp_fees, 0) AS perp_fees,
    COALESCE(auction_fees, 0) AS auction_fees,
    COALESCE(daily_burn, 0) AS daily_burn,
    COALESCE(daily_price, 0) AS daily_price,
    -- protocol’s revenue split between HLP (supplier) and AF (holder) at a ratio of 46%:54%
    COALESCE(fees, 0) * 0.46 as total_supply_side_revenue,
    -- add daily burn back to the revenue
    (COALESCE(fees, 0) * 0.54) + COALESCE(daily_burn * daily_price, 0) as revenue
from unique_traders_data
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join daily_burn_data using(date, chain)
left join daily_price_data using(date, chain)
where date < to_date(sysdate())
