{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
    treasury_by_token as (
        SELECT
            date
            , token
        , sum(usd_balance) as treasury
        , sum(native_balance) as treasury_native
    FROM {{ ref('fact_synthetix_treasury_by_token') }}
    where usd_balance > 0
    group by 1,2
    )
    , net_treasury as (
        SELECT
            date
            , token
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
            , token
            , sum(usd_balance) as own_token_treasury
            , sum(native_balance) as own_token_treasury_native
        FROM {{ ref('fact_synthetix_treasury_by_token') }}
        where token = 'SNX'
        and native_balance > 0
        group by 1,2
    ) 
    , tvl as (
        select
            date
            , case
                when lower(contract_address) = lower('0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F') then 'SNX'
                when lower(contract_address) = lower('0xb2f30a7c980f052f02563fb518dcc39e6bf38175') then 'snxUSD'
                when lower(contract_address) = lower('0x8700daec35af8ff88c16bdf0418774cb3d7599b4') then 'sUSD'
                else null
            end as token
            , sum(balance_raw) as tvl_raw
            , sum(balance_native) as tvl_native
            , sum(balance) as tvl
        from {{ ref('fact_synthetix_tvl_by_token_and_chain') }}
        group by 1,2
    )
    , date_token_spine as (
        SELECT
            distinct
            date
            , token
        from {{ ref('dim_date_spine') }}
        CROSS JOIN (
                    SELECT distinct token from treasury_by_token
                    UNION
                    SELECT distinct token from net_treasury
                    UNION
                    SELECT distinct token from treasury_native
                    UNION
                    SELECT distinct token from tvl
                    )
        where date between '2019-08-11' and to_date(sysdate())
    ), 
    token_cashflow as (
        select
            date
            , token
            , sum(fee_allocation) as token_fee_allocation
        from {{ ref("fact_synthetix_token_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , service_cashflow as (
        select
            date
            , token
            , sum(service_cashflow) as service_cashflow
        from {{ ref("fact_synthetix_service_cashflow_by_token_and_chain") }}
        group by 1,2
    )   
    , treasury_cashflow as (
        select
            date
            , symbol as token
            , sum(treasury_cashflow) as treasury_cashflow
        from {{ ref("fact_synthetix_treasury_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , fee_sharing_cashflow as (
        select
            date
            , token
            , sum(fee_sharing_fee_allocation) as fee_sharing_fee_allocation
        from {{ ref("fact_synthetix_fee_sharing_cashflow_by_token_and_chain") }}
        group by 1,2
    )
    , fees as (
        select
            date
            , token
            , sum(fees_usd) as fees_usd
            , sum(fees_native) as fees_native
        from {{ ref("fact_synthetix_fees_by_token_and_chain") }}
        group by 1,2
    )
select
    date
    , token
    , coalesce(fees.fees_usd, 0) as fees
    , coalesce(fees.fees_native, 0) as fees_native
    , coalesce(tvl.tvl, 0) as net_deposits

    -- Standardized Metrics

    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl - lag(tvl.tvl) over (order by date), 0) as tvl_net_change
    , coalesce(tvl.tvl_native, 0) as tvl_native
    , coalesce(tvl.tvl_native - lag(tvl.tvl_native) over (order by date), 0) as tvl_native_net_change

    -- Cashflow Metrics
    , coalesce(fees.fees_usd, 0) as ecosystem_revenue
    , coalesce(fees.fees_native, 0) as ecosystem_revenue_native
    , coalesce(token_cashflow.token_fee_allocation, 0) as token_fee_allocation
    , coalesce(service_cashflow.service_cashflow, 0) as service_fee_allocation
    , coalesce(treasury_cashflow.treasury_cashflow, 0) as treasury_fee_allocation
    , coalesce(fee_sharing_cashflow.fee_sharing_fee_allocation, 0) as fee_sharing_fee_allocation

    -- Protocol Metrics
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from date_token_spine
full outer join treasury_by_token using(date, token)
full outer join net_treasury using(date, token)
full outer join treasury_native using(date, token)
full outer join tvl using(date, token)
full outer join fees using(date, token)
full outer join token_cashflow using(date, token)
full outer join service_cashflow using(date, token)
full outer join treasury_cashflow using(date, token)
full outer join fee_sharing_cashflow using(date, token)
