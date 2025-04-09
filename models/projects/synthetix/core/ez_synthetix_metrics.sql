{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_synthetix_trading_volume") }}
    )
    , unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_synthetix_unique_traders") }}
    )
    , tvl as (
        select 
            date,
            sum(tvl_usd) as tvl,
            sum(tvl_usd) as net_deposits
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
        group by 1 
    )
    , token_holders as (
        select
            date,
            sum(token_holder_count) as token_holder_count
        from {{ ref('fact_synthetix_token_holders') }}
        group by 1
    )
    , fees as (
        select
            date,
            fees as fees
        from {{ ref('fact_synthetix_fees') }}
    )
    , expenses as (
        select
            date,
            daily_expenses as expenses
        from {{ ref('fact_synthetix_expenses') }}
    )
    , token_incentives as (
        select
            date,
            sum(token_incentives) as token_incentives
        from {{ ref("fact_synthetix_token_incentives_by_chain") }}
        group by 1
    )
    , treasury as (
        select 
            date
            , sum(treasury) as treasury
            , sum(treasury_native) as treasury_native
            , sum(net_treasury) as net_treasury
            , sum(net_treasury_native) as net_treasury_native
            , sum(own_token_treasury) as own_token_treasury
            , sum(own_token_treasury_native) as own_token_treasury_native
        from {{ ref('ez_synthetix_metrics_by_token') }}
        group by 1
    )
    , market_data as (
        {{ get_coingecko_metrics('havven') }}
    )
select
    date
    , 'synthetix' as app
    , 'DeFi' as category
    , coalesce(trading_volume, 0) as trading_volume
    , coalesce(unique_traders, 0) as unique_traders
    , coalesce(net_deposits, 0) as net_deposits
    , coalesce(fees, 0) as fees
    , coalesce(fees, 0) as revenue
    , coalesce(token_incentives, 0) as expenses
    , coalesce(revenue, 0) - coalesce(expenses,0) as protocol_earnings
    , coalesce(token_incentives, 0) as token_incentives
    , coalesce(treasury.treasury, 0) as treasury_value
    , coalesce(treasury.own_token_treasury, 0) as treasury_value_native
    , coalesce(treasury.net_treasury, 0) as net_treasury_value
    , coalesce(token_holders.token_holder_count, 0) as token_holder_count

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume

    -- Spot DEX Metrics
    , coalesce(unique_traders, 0) as spot_dau
    , coalesce(trading_volume, 0) as spot_volume
    , coalesce(fees, 0) as spot_revenue

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl

    -- Cash Flow Metrics
    , coalesce(fees, 0) as gross_protocol_revenue

    -- Protocol Metrics
    , coalesce(treasury.treasury, 0) as treasury
    , coalesce(treasury.treasury_native, 0) as treasury_native
    , coalesce(treasury.net_treasury, 0) as net_treasury
    , coalesce(treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury.own_token_treasury_native, 0) as own_token_treasury_native

    -- Supply Metrics
    , coalesce(token_incentives, 0) as mints

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
from unique_traders_data
left join trading_volume_data using(date)
left join tvl using(date)
left join fees using(date)
left join expenses using(date)
left join token_incentives using(date)
left join treasury using(date)
left join market_data using(date)
left join token_holders using(date)
where date < to_date(sysdate())