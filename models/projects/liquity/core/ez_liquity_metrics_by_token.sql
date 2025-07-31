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
    , 'liquity' as artemis_id
    , dts.token

    --Usage Data
    , tvl.tvl
    , tvl.tvl as lending_deposits
    , coalesce(os.outstanding_supply, 0) as lending_loans

    --Fee Data
    , fr.revenue_native as fee_native
    , fr.revenue_usd as lending_fees
    , fr.revenue_usd as fees

    --Fee Allocation
    , coalesce(ti.token_incentives_native, 0) as staking_fee_allocation_native

    --Treasury Data
    , t.treasury as treasury
    , t.net_treasury as net_treasury
    , t.own_token_treasury as own_token_treasury  


from date_token_spine dts
left join tvl using (date, token)
left join outstanding_supply os using (date, token)
left join fees_and_revs fr using (date, token)
left join treasury_by_token using (date, token)
left join net_treasury using (date, token)
left join treasury_native using (date, token)
left join token_incentives ti using (date, token)