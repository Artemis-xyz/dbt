{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics'
    )
}}

with date_spine as (
    select date
    from {{ ref('dim_date_spine') }}
    where date between '2020-03-01' and to_date(sysdate())
)
, swap_metrics as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as unique_traders,
        count(*) as number_of_swaps,
        sum(trading_volume) as trading_volume,
        sum(fee_usd) as trading_fees,
        sum(service_cash_flow) as primary_supply_side_revenue,
        sum(treasury_cash_flow + vebal_cash_flow) as revenue,
        sum(service_cash_flow) as service_cash_flow,
        sum(treasury_cash_flow) as treasury_cash_flow,
        sum(vebal_cash_flow) as vebal_cash_flow
    FROM {{ ref('ez_balancer_dex_swaps') }}
    group by 1
)
, token_incentives as (
    SELECT
        date,
        sum(amount_usd) as token_incentives_usd
    FROM {{ ref('fact_balancer_token_incentives_all_chains') }}
    group by 1
)
, all_tvl as (
    SELECT
        date,
        sum(amount_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1
)
, treasury as (
    SELECT
        date,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    group by 1
)
, treasury_native as (
    SELECT
        date,
        sum(native_balance) as treasury_native,
        sum(usd_balance) as own_token_treasury
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token = 'BAL'
    group by 1
)
, net_treasury as (
    SELECT
        date,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    group by 1
)
, token_holders as (
    SELECT
        date,
        token_holder_count
    FROM {{ ref('fact_balancer_token_holders') }}
)
, market_data as (
    {{ get_coingecko_metrics('balancer') }}
)
select
    date_spine.date,
    coalesce(swap_metrics.unique_traders, 0) as unique_traders,
    coalesce(swap_metrics.number_of_swaps, 0) as number_of_swaps,
    coalesce(swap_metrics.trading_volume, 0) as trading_volume,
    coalesce(swap_metrics.trading_fees, 0) as trading_fees,
    coalesce(swap_metrics.trading_fees, 0) as fees,
    coalesce(swap_metrics.primary_supply_side_revenue, 0) as primary_supply_side_revenue,
    coalesce(swap_metrics.revenue, 0) as revenue,
    coalesce(token_incentives.token_incentives_usd, 0) as expenses,
    coalesce(swap_metrics.revenue, 0) - coalesce(token_incentives.token_incentives_usd, 0) as earnings,
    coalesce(all_tvl.tvl_usd, 0) as net_deposits,
    coalesce(treasury.net_treasury_usd, 0) as treasury_value,
    coalesce(net_treasury.net_treasury_usd, 0) as net_treasury_value,
    coalesce(treasury_native.treasury_native, 0) as treasury_value_native

    -- Standardized Metrics
    -- Market Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume

    -- Usage/Sector Metrics
    , coalesce(swap_metrics.unique_traders, 0) as spot_dau
    , coalesce(swap_metrics.number_of_swaps, 0) as spot_txns
    , coalesce(swap_metrics.trading_volume, 0) as spot_volume
    , coalesce(all_tvl.tvl_usd, 0) as tvl

    -- Money Metrics
    , coalesce(swap_metrics.trading_fees, 0) as spot_fees
    , coalesce(swap_metrics.trading_fees, 0) as ecosystem_revenue
    , coalesce(swap_metrics.service_cash_flow, 0) as service_cash_flow
    , coalesce(swap_metrics.treasury_cash_flow, 0) as treasury_cash_flow
    , coalesce(swap_metrics.vebal_cash_flow, 0) as staking_cash_flow
    , coalesce(token_incentives.token_incentives_usd, 0) as token_incentives

    -- Treasury Metrics
    , coalesce(treasury.net_treasury_usd, 0) as treasury
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(net_treasury.net_treasury_usd, 0) as net_treasury

    -- Other Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(token_holders.token_holder_count, 0) as tokenholder_count

from date_spine
left join all_tvl using (date)
left join treasury using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join token_holders using (date)
left join market_data using (date)
left join swap_metrics using (date)
left join token_incentives using (date)