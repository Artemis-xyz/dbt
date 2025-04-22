{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics",
    )
}}

with trading_volume_data as (
    select 
        date, 
        chain,
        trading_volume
    from {{ ref("fact_hyperliquid_trading_volume") }}
)
, unique_traders_data as (
    select 
        date, 
        chain,
        unique_traders
    from {{ ref("fact_hyperliquid_unique_traders") }}
)
, daily_transactions_data as (
    select 
        date, 
        chain,
        trades
    from {{ ref("fact_hyperliquid_daily_transactions") }}
)
, fees_data as (
    select 
        date, 
        chain, 
        trading_fees, 
        spot_fees, 
        perp_fees
    from {{ ref("fact_hyperliquid_fees") }}
)
, auction_fees_data as (
    select 
        date, 
        chain, 
        sum(auction_fees) as auction_fees
    from {{ ref("fact_hyperliquid_auction_fees") }}
    group by 1, 2
)
, daily_burn_data as (
    select 
        date, 
        chain, 
        daily_burn
    from {{ ref("fact_hyperliquid_daily_burn") }}
)
, daily_supply_data as (
    select
        date,
        emissions_native,
        premine_unlocks_native,
    from {{ref('fact_hyperliquid_daily_supply_data')}}
)
, date_spine as (
    select * from {{ ref('dim_date_spine') }}
    where date between '2022-12-20' and to_date(sysdate())
)
, market_metrics as (
    ({{ get_coingecko_metrics("hyperliquid") }}) 
)
select
    date_spine.date
    , 'hyperliquid' as app
    , 'DeFi' as category

    --Old metrics needed for compatibility
    , trading_volume
    , unique_traders::string as unique_traders
    , trades as txns
    , trading_fees as fees
    , auction_fees
    , daily_burn
    , COALESCE(trading_fees * 0.46, 0) as primary_supply_side_revenue -- protocol’s revenue split between HLP (supplier) and AF (holder) at a ratio of 46%:54%
    , COALESCE(trading_fees * 0.54, 0) + COALESCE(daily_burn, 0) * market_metrics.price as revenue -- add daily burn back to the revenue

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    -- Usage Metrics
    , unique_traders_data.unique_traders::string as perp_dau
    , trading_volume_data.trading_volume as perp_volume
    , daily_transactions_data.trades as perp_txns

    -- Cash Flow Metrics
    , fees_data.perp_fees
    , fees_data.spot_fees
    , fees_data.perp_fees + fees_data.spot_fees as trading_fees
    , daily_burn_data.daily_burn * market_metrics.price as chain_fees
    , trading_fees + chain_fees as gross_protocol_revenue
    , trading_fees * 0.46 as service_cash_flow
    , trading_fees * 0.54 as buybacks_cash_flow
    , daily_burn_data.daily_burn as burned_cash_flow_native
    , chain_fees as burned_cash_flow

    --HYPE Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_burn_data.daily_burn, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_burn_data.daily_burn, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_burn_data.daily_burn, 0)) over (order by daily_supply_data.date) as circulating_supply_native

    -- Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv
from date_spine
left join market_metrics using(date)
left join unique_traders_data using(date)
left join trading_volume_data using(date)
left join daily_transactions_data using(date)
left join fees_data using(date)
left join daily_burn_data using(date)
left join daily_supply_data using(date)
left join auction_fees_data using(date)
where date < to_date(sysdate())