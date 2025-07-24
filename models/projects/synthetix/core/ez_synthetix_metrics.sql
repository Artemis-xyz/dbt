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
            sum(fee_sharing_fee_allocation) as fee_sharing_fee_allocation
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
    , 'synthetix' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price as price
    , market_data.market_cap as market_cap
    , market_data.fdmc as fdmc
    , market_data.token_volume as token_volume

    -- Usage Data
    , unique_traders as perp_dau
    , unique_traders as dau
    , tvl as perp_tvl
    , tvl as tvl
    , tvl - lag(tvl) over (order by date) as tvl_net_change

    -- Fee Data
    , fees.fees_native as fees_native
    , fees.fees as perp_fees
    , fees.fees as fees
    , token_cashflow as tokenholder_fee_allocation
    , service_cashflow as lp_fee_allocation
    , treasury_cashflow as dao_fee_allocation
    , fee_sharing_fee_allocation as staking_fee_allocation

    -- Financial Statements
    , coalesce(token_cashflow, 0) + coalesce(treasury_cashflow, 0) + coalesce(fee_sharing_fee_allocation, 0) as revenue
    , token_incentives as token_incentives
    , coalesce(revenue, 0) - coalesce(token_incentives,0) as earnings

    -- Treasury Data
    , treasury.treasury as treasury
    , treasury.net_treasury as net_treasury
    , treasury.own_token_treasury as own_token_treasury

    -- Supply Metrics
    , mints as gross_emissions
    , mints_native as gross_emissions_native

    -- Turnover Metrics
    , market_data.token_turnover_circulating as token_turnover_circulating
    , market_data.token_turnover_fdv as token_turnover_fdv
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