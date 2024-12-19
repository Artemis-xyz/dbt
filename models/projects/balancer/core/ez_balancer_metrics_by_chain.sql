{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)

, all_tvl_by_chain as (
    SELECT
        date,
        chain,
        sum(tvl_native) as tvl_native,
        sum(tvl_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1,2
)

select
    date_spine.date,
    all_tvl_by_chain.chain,
    all_tvl_by_chain.tvl_native,
    all_tvl_by_chain.tvl_usd
from date_spine
full outer join all_tvl_by_chain on date_spine.date = all_tvl_by_chain.date