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
    )
    , unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_hyperliquid_unique_traders") }}
    )
    , daily_transactions_data as (
        select date, trades, chain
        from {{ ref("fact_hyperliquid_daily_transactions") }}
    )
    , fees_data as (
        select date, chain, trading_fees, spot_fees, perp_fees
        from {{ ref("fact_hyperliquid_fees") }}
    )
    , auction_fees_data as (
        select date, chain, sum(auction_fees) as auction_fees
        from {{ ref("fact_hyperliquid_auction_fees") }}
        group by 1, 2
    )
    , daily_burn_data as (
        select date, daily_burn, chain
        from {{ ref("fact_hyperliquid_daily_burn") }}
    ),
    price as (
        select * from ({{ get_coingecko_price_with_latest("hyperliquid") }}) 
    ),
    date_spine as (
        SELECT
            date,
            'hyperliquid' as chain
        FROM {{ref("dim_date_spine")}}
        WHERE date between '2023-06-13' and to_date(sysdate())
    )
select
    date
    , 'hyperliquid' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders::string as unique_traders
    , trades as txns
    , trading_fees as fees
    , spot_fees
    , perp_fees
    , auction_fees
    , daily_burn
    -- protocol’s revenue split between HLP (supplier) and AF (holder) at a ratio of 46%:54%
    , COALESCE(trading_fees * 0.46, 0) as primary_supply_side_revenue
    -- add daily burn back to the revenue
    , COALESCE(trading_fees * 0.54, 0) + COALESCE(daily_burn, 0) * mm.price as revenue

    -- Standardized Metrics
    , unique_traders::string as perp_dau
    , trading_volume as perp_volume
    , trades as perp_txns

    -- Revenue Metrics
    , trading_fees + (daily_burn * mm.price) as gross_protocol_revenue
    , perp_fees as perp_revenue
    , spot_fees as spot_revenue
    , trading_fees * 0.46 as service_cash_flow
    , trading_fees * 0.54 as buybacks_cash_flow
    , daily_burn as l1_cash_flow_native
    , daily_burn * mm.price as l1_cash_flow

    -- Market metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
from date_spine
left join unique_traders_data using(date, chain)
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join daily_burn_data using(date, chain)
left join market_metrics mm using(date)
where date < to_date(sysdate())