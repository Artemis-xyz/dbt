{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with treasury_by_chain as (
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
        --SELECT distinct chain from all_tvl_by_chain
        --UNION
        SELECT distinct chain from treasury_native
        UNION
        SELECT distinct chain from net_treasury
    )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_chain_spine.date,
    date_chain_spine.chain,
    trading_metrics_by_chain.version,
    trading_metrics_by_chain.swap_count,
    trading_metrics_by_chain.trading_fees,
    trading_metrics_by_chain.fees,
    trading_metrics_by_chain.primary_supply_side_revenue,
    trading_metrics_by_chain.secondary_supply_side_revenue,
    trading_metrics_by_chain.total_supply_side_revenue,
    trading_metrics_by_chain.protocol_revenue,
    trading_metrics_by_chain.operating_expenses,
    trading_metrics_by_chain.token_incentives,
    trading_metrics_by_chain.protocol_earnings,
    --all_tvl_by_chain.tvl_usd as tvl,
    tvl_balancer_v1.tvl_usd,
    treasury_by_chain.usd_balance as treasury_value,
    treasury_native.treasury_native as treasury_native,
    net_treasury.net_treasury_usd as net_treasury_value,
    trading_metrics_by_chain.trading_volume,
    trading_metrics_by_chain.unique_traders
from date_chain_spine
--left join all_tvl_by_chain using (date, chain)
left join treasury_by_chain using (date, chain)
left join treasury_native using (date, chain)
left join net_treasury using (date, chain)
left join trading_metrics_by_chain using (date, chain)
left join tvl_balancer_v1 using (date, chain)