{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with  fees_and_revs as (
    select
        date,
        token,
        sum(revenue_usd) as revenue_usd,
        sum(revenue_native) as revenue_native
    from {{ ref('fact_liquity_fees_and_revs') }}
    group by 1, 2
)
, outstanding_supply as (
    select
        date,
        token,
        sum(outstanding_supply) as outstanding_supply
    from {{ ref('fact_liquity_outstanding_supply') }}
    group by 1, 2
)
, treasury_by_token as (
    select
        date,
        token,
        sum(usd_balance) as treasury,
        sum(native_balance) as treasury_native
    from {{ ref('fact_liquity_treasury') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        token,
        sum(usd_balance) as net_treasury,
        sum(native_balance) as net_treasury_native
    from {{ ref('fact_liquity_treasury') }}
    where token != 'LQTY'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        token,
        sum(usd_balance) as own_token_treasury,
        sum(native_balance) as own_token_treasury_native
    from {{ ref('fact_liquity_treasury') }}
    where token = 'LQTY'
    group by 1, 2
)  
, token_incentives as (
    select
        date,
        token,
        sum(token_incentives_native) as token_incentives_native
    from {{ ref('fact_liquity_token_incentives') }}
    group by 1, 2
)
, tvl as (
    select
        date,
        token,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
    group by 1, 2
)
, date_token_spine as (
    select
        date,
        token
    from {{ ref('dim_date_spine') }}
    cross join (select distinct token from tvl
                union
                select distinct token from outstanding_supply
                union
                select distinct token from fees_and_revs
                union
                select distinct token from token_incentives
                union
                select distinct token from treasury_by_token
                union
                select distinct token from net_treasury
                union
                select distinct token from treasury_native
                union
                select distinct token from tvl)
    where date between '2021-04-05' and to_date(sysdate())
)

select
    dts.date
    , dts.token
    , fr.revenue_usd as fees
    , fr.revenue_native as fees_native
    , fr.revenue_usd as revenue
    , fr.revenue_native as revenue_native
    , ti.token_incentives_native as token_incentives_native
    , ti.token_incentives_native as expenses_native
    , treasury_by_token.treasury as treasury_value
    , treasury_by_token.treasury_native as treasury_native_value
    , net_treasury.net_treasury as net_treasury_value
    , os.outstanding_supply

    -- Standardized Metrics

    -- Lending Metrics
    , tvl.tvl as lending_deposits
    , fr.revenue_usd as lending_fees
    , os.outstanding_supply as lending_loans

    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , fr.revenue_usd as ecosystem_revenue
    , fr.revenue_native as ecosystem_revenue_native
    , ti.token_incentives_native as staking_fee_allocation_native

    -- Protocol Metrics
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from date_token_spine dts
left join tvl using (date, token)
left join outstanding_supply os using (date, token)
left join fees_and_revs fr using (date, token)
left join treasury_by_token using (date, token)
left join net_treasury using (date, token)
left join treasury_native using (date, token)
left join token_incentives ti using (date, token)