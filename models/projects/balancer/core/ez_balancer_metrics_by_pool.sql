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
        sum(treasury_cash_flow + vebal_cash_flow) as revenue,
        sum(service_cash_flow) as primary_supply_side_revenue,
        sum(service_cash_flow) as service_cash_flow,
        sum(treasury_cash_flow) as treasury_cash_flow,
        sum(vebal_cash_flow) as staking_cash_flow
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
    , swap_metrics.revenue
    , swap_metrics.primary_supply_side_revenue
    , swap_metrics.primary_supply_side_revenue as total_supply_side_revenue
    , coalesce(tvl.tvl_usd, 0) as net_deposits

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , coalesce(swap_metrics.unique_traders, 0) as spot_dau
    , coalesce(swap_metrics.number_of_swaps, 0) as spot_txns
    , coalesce(swap_metrics.trading_volume, 0) as spot_volume
    , coalesce(swap_metrics.trading_fees, 0) as spot_fees
    , coalesce(tvl.tvl_usd, 0) as tvl

    -- Money Metrics
    , coalesce(swap_metrics.trading_fees, 0) as ecosystem_revenue
    , coalesce(swap_metrics.service_cash_flow, 0) as service_cash_flow
    , coalesce(swap_metrics.treasury_cash_flow, 0) as treasury_cash_flow
    , coalesce(swap_metrics.staking_cash_flow, 0) as staking_cash_flow
from date_pool_spine
left join swap_metrics using (date, pool_address)
left join tvl using (date, pool_address)