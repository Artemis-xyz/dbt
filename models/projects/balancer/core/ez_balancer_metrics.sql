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
        sum(amount_in_usd) as trading_volume,
        sum(fee_usd) as trading_fees,
        sum(supply_side_revenue_usd) as primary_supply_side_revenue,
        sum(revenue) as revenue
    FROM {{ ref('ez_balancer_dex_swaps') }}
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
        sum(native_balance) as treasury_native
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
    swap_metrics.unique_traders,
    swap_metrics.number_of_swaps,
    swap_metrics.trading_volume,
    swap_metrics.trading_fees,
    swap_metrics.trading_fees as fees,
    swap_metrics.primary_supply_side_revenue,
    swap_metrics.revenue,
    all_tvl.tvl_usd as tvl,
    treasury.net_treasury_usd as treasury_value,
    net_treasury.net_treasury_usd as net_treasury_value,
    treasury_native.treasury_native as treasury_native,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume,
    token_holders.token_holder_count
from date_spine
left join all_tvl using (date)
left join treasury using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join token_holders using (date)
left join market_data using (date)
left join swap_metrics using (date)