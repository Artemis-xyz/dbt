{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with all_tvl_by_chain as (
    SELECT
        date,
        chain,
        sum(tvl_native) as tvl_native,
        sum(tvl_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1,2
)
, treasury_by_chain as (
    SELECT
        date,
        'ethereum' as chain,
        sum(usd_balance) as usd_balance
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    group by 1,2
)
, net_treasury as(
    SELECT
        date,
        'ethereum' as chain,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        chain,
        sum(native_balance) as treasury_native
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token = 'BAL'
    group by 1,2
)
, date_chain_spine as (
    SELECT
        distinct
        date,
        chain
    FROM {{ ref('dim_date_spine') }}
    CROSS JOIN (SELECT distinct chain from treasury_by_chain
        UNION
        SELECT distinct chain from all_tvl_by_chain
        UNION
        SELECT distinct chain from treasury_native
        UNION
        SELECT distinct chain from net_treasury
    )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_chain_spine.date,
    date_chain_spine.chain,
    all_tvl_by_chain.tvl_usd as tvl,
    treasury_by_chain.usd_balance as treasury_value,
    net_treasury.net_treasury_usd as net_treasury_value,
    treasury_native.treasury_native as treasury_native
from date_chain_spine
left join all_tvl_by_chain using (date, chain)
left join treasury_by_chain using (date, chain)
left join treasury_native using (date, chain)
left join net_treasury using (date, chain)
