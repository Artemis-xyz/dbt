{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='core',
        alias='ez_liquity_metrics_by_token'
    )
}}

with tvl as (
    select
        date,
        token,
        sum(tvl_usd) as tvl
    from {{ ref('fact_liquity_tvl') }}
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
, fees_and_revs as (
    select
        date,
        token,
        sum(revenue_usd) as revenue_usd,
        sum(revenue_native) as revenue_native
    from {{ ref('fact_liquity_fees_and_revs') }}
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
                select distinct token from fees_and_revs)
    where date between '2021-04-05' and to_date(sysdate())
)

select
    dts.date,
    dts.token,
    tvl.tvl,
    os.outstanding_supply,
    fr.revenue_usd as fees,
    fr.revenue_native as fees_native,
    fr.revenue_usd as revenue,
    fr.revenue_native as revenue_native
from date_token_spine dts
left join tvl using (date, token)
left join outstanding_supply os using (date, token)
left join fees_and_revs fr using (date, token)