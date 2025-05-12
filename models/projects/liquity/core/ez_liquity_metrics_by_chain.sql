{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with fees_and_revs as (
    select
        date,
        chain,
        sum(revenue_usd) as revenue_usd
    from {{ ref('fact_liquity_fees_and_revs') }}
    group by 1, 2
)
, token_incentives as (
    select
        date,
        chain,
        sum(token_incentives) as token_incentives
    from {{ ref('fact_liquity_token_incentives') }}
    group by 1, 2
)
, treasury as (
    select
        date,
        chain,
        sum(usd_balance) as treasury
    from {{ ref('fact_liquity_treasury') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        chain,
        sum(usd_balance) as net_treasury
    from {{ ref('fact_liquity_treasury') }}
    where token != 'LQTY'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        chain,
        sum(usd_balance) as own_token_treasury
    from {{ ref('fact_liquity_treasury') }}
    where token = 'LQTY'
    group by 1, 2
)  
, tvl as (
    select
        date,
        chain,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
    group by 1, 2
) 
, outstanding_supply as (
    select
        date,
        chain,
        sum(outstanding_supply) as outstanding_supply
    from {{ ref('fact_liquity_outstanding_supply') }}
    group by 1, 2
)
, date_chain_spine as (
    select
        date,
        chain
    from {{ ref('dim_date_spine') }}
    cross join (select distinct chain from tvl
                union
                select distinct chain from outstanding_supply
                union
                select distinct chain from fees_and_revs)
    where date between '2021-04-05' and to_date(sysdate())
)

select
    dcs.date
    , dcs.chain
    , coalesce(fr.revenue_usd, 0) as fees
    , coalesce(fr.revenue_usd, 0) as revenue
    , coalesce(ti.token_incentives, 0) as token_incentives
    , coalesce(ti.token_incentives, 0) as expenses
    , coalesce(fr.revenue_usd, 0) - coalesce(ti.token_incentives, 0) as protocol_earnings
    , coalesce(treasury.treasury, 0) as treasury_value
    , coalesce(treasury_native.own_token_treasury, 0) as treasury_value_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury_value
    , coalesce(os.outstanding_supply, 0) as outstanding_supply
    , coalesce(tvl.tvl, 0) as net_deposits

    -- Standardized Metrics

    -- Lending Metrics
    , coalesce(tvl.tvl, 0) as lending_deposits
    , coalesce(fr.revenue_usd, 0) as lending_fees
    , coalesce(os.outstanding_supply, 0) as lending_loans

    -- Crypto Metrics
    , coalesce(tvl.tvl, 0) as tvl
    , coalesce(tvl.tvl, 0) - lag(coalesce(tvl.tvl, 0)) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , coalesce(fr.revenue_usd, 0) as gross_protocol_revenue
    , coalesce(ti.token_incentives, 0) as fee_sharing_token_cash_flow

    -- Protocol Metrics
    , coalesce(treasury.treasury, 0) as treasury
    , coalesce(treasury_native.own_token_treasury, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
from date_chain_spine dcs
left join tvl using (date, chain)
left join outstanding_supply os using (date, chain)
left join fees_and_revs fr using (date, chain)  
left join treasury using (date, chain)
left join net_treasury using (date, chain)
left join treasury_native using (date, chain)
left join token_incentives ti using (date, chain)