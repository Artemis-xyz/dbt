{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics",
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
    )
    , assistance_fund_data as (
        select date, cumulative_hype, usd_value as hype_usd, hype_value, chain
        from {{ ref("fact_hyperliquid_assistance_fund_data") }}
    )
    , daily_assistance_fund_data as (
        select date, daily_balance as daily_buybacks_native, balance as assistance_fund_balance, chain
        from {{ ref("fact_hyperliquid_assistance_fund_balance") }}
    )
    , market_metrics as (
        ({{ get_coingecko_metrics("hyperliquid") }}) 
    )
select
    date
    , 'hyperliquid' as app
    , 'DeFi' as category
    , trading_volume
    , unique_traders::string as unique_traders
    , trades as txns
    , trading_fees as fees
    , auction_fees
    , daily_burn
    , trading_fees * 0.03 as primary_supply_side_revenue
    -- add daily burn back to the revenue
    , case
        when date >= '2025-02-01' then hype_usd + daily_burn * mm.price
        else (daily_buybacks_native * mm.price) + (daily_burn * mm.price)
    end as revenue
    , daily_buybacks_native

    -- Standardized Metrics
    , unique_traders::string as perp_dau
    , trading_volume as perp_volume
    , trades as perp_txns

    -- Revenue Metrics
    , perp_fees
    , spot_fees
    -- all l1 fees are burned
    , daily_burn * mm.price as chain_fees
    , trading_fees + (daily_burn * mm.price) as gross_protocol_revenue
    , trading_fees * 0.03 as service_cash_flow
    , case
        when date >= '2025-02-01' then hype_usd
        else daily_buybacks_native * mm.price
    end as buybacks_cash_flow
    , daily_buybacks_native as buybacks_native
    , daily_burn as burned_cash_flow_native
    , daily_burn * mm.price as burned_cash_flow

    -- Market metrics
    , mm.price as price
    , mm.token_volume as token_volume
    , mm.market_cap as market_cap
    , mm.fdmc as fdmc
    , mm.token_turnover_circulating as token_turnover_circulating
    , mm.token_turnover_fdv as token_turnover_fdv

from unique_traders_data
left join trading_volume_data using(date, chain)
left join daily_transactions_data using(date, chain)
left join fees_data using(date, chain)
left join auction_fees_data using(date, chain)
left join daily_burn_data using(date, chain)
left join assistance_fund_data using(date, chain)
left join daily_assistance_fund_data using(date, chain)
left join market_metrics mm using(date)
where date < to_date(sysdate())