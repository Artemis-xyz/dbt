{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_pool'
    )
}}

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        pool_address,
        count(*) as number_of_swaps,
        count(distinct sender) as unique_traders,
        sum(trading_volume) as trading_volume,
        sum(fee_usd) as trading_fees,
        sum(revenue) as revenue
    FROM {{ ref('ez_balancer_dex_swaps') }}
    group by 1,2
)
, tvl as (
    SELECT
        date,
        pool_address,
        sum(amount_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1,2
)
, date_pool_spine as (
    SELECT distinct date, pool_address
    FROM {{ ref('dim_date_spine') }}
    cross join (
        SELECT distinct pool_address FROM tvl
        UNION
        SELECT distinct pool_address FROM swap_metrics
    )
)

select
    date_pool_spine.date
    , date_pool_spine.pool_address
    , swap_metrics.number_of_swaps
    , swap_metrics.unique_traders
    , swap_metrics.trading_volume
    , swap_metrics.trading_fees
    , swap_metrics.revenue
    , tvl.tvl_usd
    , tvl.tvl_usd as net_deposits
from date_pool_spine
left join swap_metrics using (date, pool_address)
left join tvl using (date, pool_address)