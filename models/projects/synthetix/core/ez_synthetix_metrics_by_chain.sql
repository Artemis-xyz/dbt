{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date
        , chain
        , trading_volume
        from {{ ref("fact_synthetix_trading_volume") }}
    ),
    unique_traders_data as (
        select date
        , chain
        , unique_traders
        from {{ ref("fact_synthetix_unique_traders") }}
    ), 
    tvl as (
        select 
            date
            , chain
            , sum(balance_raw) as tvl_raw
            , sum(balance_native) as tvl_native
            , sum(balance) as tvl
        from {{ ref("fact_synthetix_tvl_by_token_and_chain") }}
        group by 1,2 
    ), 
    token_incentives as (
        select
            date
            , chain
            , sum(token_incentives) as token_incentives
        from {{ ref("fact_synthetix_token_incentives_by_chain") }}
        group by 1,2
    ),
    token_holders as (
        select
            date
            , chain
            , token_holder_count
        from {{ ref("fact_synthetix_tokenholders_by_chain") }}
    )
    , treasury_by_chain as (
        SELECT
            date
            , chain
        , sum(usd_balance) as treasury
        , sum(native_balance) as treasury_native
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
    )
    , net_treasury as (
        SELECT
            date
            , chain
            , sum(usd_balance) as net_treasury
            , sum(native_balance) as net_treasury_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token <> 'SNX'
        and usd_balance > 0
        group by 1,2
    )
    , treasury_native as (
        SELECT
            date
            , chain
            , sum(usd_balance) as own_token_treasury
            , sum(native_balance) as own_token_treasury_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token = 'SNX'
        and native_balance > 0
        group by 1,2
    ) 
    , fees as (
        select
            date
            , chain
            , sum(fees_usd) as fees_usd
            , sum(fees_native) as fees_native
        from {{ ref("fact_synthetix_fees_by_token_and_chain") }}
        group by 1,2
    )
    , service_cashflow as (
        select
            date
            , chain
            , sum(service_cashflow) as service_cashflow
        from {{ ref("fact_synthetix_service_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , treasury_cashflow as (
        select
            date
            , chain
            , sum(treasury_cashflow) as treasury_cashflow
        from {{ ref("fact_synthetix_treasury_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , fee_sharing_cashflow as (
        select
            date
            , chain
            , sum(fee_sharing_cash_flow) as fee_sharing_cashflow
        from {{ ref("fact_synthetix_fee_sharing_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , token_cashflow as (
        select
            date
            , chain
            , sum(cash_flow) as token_cashflow
        from {{ ref("fact_synthetix_token_cashflow_by_token_and_chain") }}
        group by 1,2
    )
select
    date
    , 'synthetix' as app
    , 'DeFi' as category
    , chain
    , coalesce(trading_volume, 0) as trading_volume
    , coalesce(unique_traders, 0) as unique_traders
    , coalesce(fees.fees_usd, 0) as fees
    , coalesce(fees.fees_native, 0) as fees_native
    , coalesce(token_incentives, 0) as token_incentives
    , coalesce(token_incentives, 0) as total_expenses
    , coalesce(fees.fees_usd, 0) - coalesce(token_incentives, 0) AS protocol_earnings
    , coalesce(token_holder_count, 0) as token_holder_count
    , coalesce(tvl.tvl, 0) as net_deposits

    -- Standardized Metrics

    -- Perpetuals Metrics
    , coalesce(unique_traders, 0) as perp_dau
    , coalesce(trading_volume, 0) as perp_volume
    
    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl - lag(tvl.tvl) over (order by date), 0) as tvl_net_change
    , coalesce(tvl.tvl_native, 0) as tvl_native
    , coalesce(tvl.tvl_native - lag(tvl.tvl_native) over (order by date), 0) as tvl_native_net_change

    -- Cashflow Metrics
    , coalesce(fees.fees_usd, 0) as gross_protocol_revenue
    , coalesce(fees.fees_native, 0) as gross_protocol_revenue_native
    , coalesce(service_cashflow.service_cashflow, 0) as service_cashflow
    , coalesce(treasury_cashflow.treasury_cashflow, 0) as treasury_cashflow
    , coalesce(fee_sharing_cashflow.fee_sharing_cashflow, 0) as fee_sharing_cashflow
    , coalesce(token_cashflow.token_cashflow, 0) as token_cashflow

    -- Protocol Metrics
    , coalesce(treasury_by_chain.treasury, 0) as treasury
    , coalesce(treasury_by_chain.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from unique_traders_data
left join trading_volume_data using(date, chain)
left join tvl using(date, chain)
left join token_incentives using(date, chain)
left join token_holders using(date, chain)
left join treasury_by_chain using(date, chain)
left join net_treasury using(date, chain)
left join treasury_native using(date, chain)
left join fees using(date, chain)
left join service_cashflow using(date, chain)
left join treasury_cashflow using(date, chain)
left join fee_sharing_cashflow using(date, chain)
left join token_cashflow using(date, chain)
where date < to_date(sysdate())