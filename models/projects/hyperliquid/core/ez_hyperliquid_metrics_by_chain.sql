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
        SELECT date(timestamp) AS date, chain, trading_fees, spot_fees, perp_fees
        FROM {{ ref("fact_hyperliquid_fees") }}
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
    trades as txns,
    trading_fees as fees,
    spot_fees,
    perp_fees,
    auction_fees,
    daily_burn,
    daily_price,
    -- protocol’s revenue split between HLP (supplier) and AF (holder) at a ratio of 46%:54%
    COALESCE(trading_fees * 0.46, 0) as primary_supply_side_revenue,
    -- add daily burn back to the revenue
    COALESCE(trading_fees * 0.54, 0) + COALESCE(daily_burn, 0) * daily_price as revenue
from unique_traders_data
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join daily_burn_data using(date, chain)
left join daily_price_data using(date, chain)
where date < to_date(sysdate())
