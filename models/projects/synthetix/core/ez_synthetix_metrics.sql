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
            date
            , sum(balance_raw) as tvl_raw
            , sum(balance_native) as tvl_native
            , sum(balance) as tvl
        from {{ ref("fact_synthetix_tvl_by_token_and_chain") }}
        group by 1 
    )
    , token_holders as (
        select
            date,
            sum(token_holder_count) as token_holder_count
        from {{ ref('fact_synthetix_tokenholders_by_chain') }}
        group by 1
    )
    , fees as (
        select
            date,
            sum(fees_usd) as fees,
            sum(fees_native) as fees_native
        from {{ ref('fact_synthetix_fees_by_token_and_chain') }}
        group by 1
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
    , token_cashflow as (
        select
            date,
            sum(fee_allocation) as token_cashflow
        from {{ ref("fact_synthetix_token_cashflow_by_token_and_chain") }}
        group by 1
    )
    , service_cashflow as (
        select
            date,
            sum(service_cashflow) as service_cashflow
        from {{ ref("fact_synthetix_service_cashflow_by_token_and_chain") }}
        group by 1
    )
    , treasury_cashflow as (
        select
            date,
            sum(treasury_cashflow) as treasury_cashflow
        from {{ ref("fact_synthetix_treasury_cashflow_by_token_and_chain") }}
        group by 1
    )
    , fee_sharing_cashflow as (
        select
            date,
            sum(fee_sharing_fee_allocation) as fee_sharing_cashflow
        from {{ ref("fact_synthetix_fee_sharing_cashflow_by_token_and_chain") }}
        group by 1
    )
    , mints as (
        select 
            date, 
            sum(mints) as mints, 
            sum(mints_native) as mints_native
        from {{ ref("fact_synthetix_snx_mints") }}
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
    , coalesce(unique_traders, 0) as dau
    , coalesce(unique_traders, 0) as unique_traders
    , coalesce(tvl, 0) as net_deposits
    , coalesce(fees.fees, 0) as fees
    , coalesce(fees.fees_native, 0) as fees_native
    , coalesce(fees.fees, 0) as revenue
    , coalesce(token_incentives, 0) as expenses
    , coalesce(revenue, 0) - coalesce(expenses,0) as earnings
    , coalesce(token_incentives, 0) as token_incentives
    , coalesce(treasury.treasury, 0) as treasury_value
    , coalesce(treasury.own_token_treasury, 0) as treasury_value_native
    , coalesce(treasury.net_treasury, 0) as net_treasury_value
    , coalesce(token_holders.token_holder_count, 0) as tokenholder_count

    -- Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume

    -- Perpetuals Metrics
    , coalesce(unique_traders, 0) as perp_dau
    , coalesce(trading_volume, 0) as perp_volume
    , coalesce(fees.fees, 0) as perp_revenue

    -- Crypto Metrics
    , coalesce(tvl, 0) as tvl
    , coalesce(tvl - lag(tvl) over (order by date), 0) as tvl_net_change
    , coalesce(tvl_native, 0) as tvl_native
    , coalesce(tvl_native - lag(tvl_native) over (order by date), 0) as tvl_native_net_change

    -- Cash Flow Metrics
    , coalesce(fees.fees, 0) as ecosystem_revenue
    , coalesce(fees.fees_native, 0) as ecosystem_revenue_native
    , coalesce(token_cashflow, 0) as token_cashflow
    , coalesce(service_cashflow, 0) as service_cashflow
    , coalesce(treasury_cashflow, 0) as treasury_cashflow
    , coalesce(fee_sharing_cashflow, 0) as fee_sharing_cashflow

    -- Protocol Metrics
    , coalesce(treasury.treasury, 0) as treasury
    , coalesce(treasury.treasury_native, 0) as treasury_native
    , coalesce(treasury.net_treasury, 0) as net_treasury
    , coalesce(treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury.own_token_treasury_native, 0) as own_token_treasury_native

    -- Supply Metrics
    , coalesce(mints, 0) as gross_emissions
    , coalesce(mints_native, 0) as gross_emissions_native

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
from unique_traders_data
left join trading_volume_data using(date)
left join tvl using(date)
left join fees using(date)
left join token_incentives using(date)
left join treasury using(date)
left join market_data using(date)
left join token_holders using(date)
left join token_cashflow using(date)
left join service_cashflow using(date)
left join treasury_cashflow using(date)
left join fee_sharing_cashflow using(date)
left join mints using(date)
where date < to_date(sysdate())