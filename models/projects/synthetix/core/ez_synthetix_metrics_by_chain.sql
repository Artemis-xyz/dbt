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
            , sum(tvl_usd) as tvl
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
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
        from {{ ref("fact_synthetix_treasury_inflow_by_token") }}
        group by 1,2
    )
    , fee_sharing_cashflow as (
        select
            date
            , chain
            , sum(fee_sharing_cashflow) as fee_sharing_cashflow
        from {{ ref("fact_synthetix_fee_sharing_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , token_cashflow as (
        select
            date
            , chain
            , sum(token_cashflow) as token_cashflow
        from {{ ref("fact_synthetix_token_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , token_incentives as (
        select
            date
            , chain
            , sum(token_incentives) as token_incentives
        from {{ ref("fact_synthetix_token_incentives_by_chain") }}
        group by 1,2
    )
select
    date
    , 'synthetix' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    , token_incentives
    , token_incentives as expenses
    , token_holder_count
    , tvl as net_deposits

    -- Standardized Metrics

    -- Spot DEX Metrics
    , unique_traders as spot_dau
    , trading_volume as spot_volume
    
    -- Crypto Metrics
    , tvl 

    -- Cashflow Metrics
    , fees_usd as gross_protocol_revenue
    , fees_native as gross_protocol_revenue_native
    , service_cashflow as service_cashflow
    , treasury_cashflow as treasury_cashflow
    , fee_sharing_cashflow as fee_sharing_cashflow
    , token_cashflow as token_cashflow
    -- Protocol Metrics
    , treasury_by_chain.treasury as treasury
    , treasury_by_chain.treasury_native as treasury_native
    , net_treasury.net_treasury as net_treasury
    , net_treasury.net_treasury_native as net_treasury_native
    , treasury_native.own_token_treasury as own_token_treasury
    , treasury_native.own_token_treasury_native as own_token_treasury_native
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
left join token_cashflow using(date, chain)
left join fee_sharing_cashflow using(date, chain)
where date < to_date(sysdate())