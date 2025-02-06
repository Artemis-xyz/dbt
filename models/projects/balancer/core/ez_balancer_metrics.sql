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

, all_tvl as (
    SELECT
        date,
        sum(tvl_usd) as tvl_usd
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
    trading_metrics.version,
    trading_metrics.swap_count,
    trading_metrics.trading_fees,
    trading_metrics.fees,
    trading_metrics.primary_supply_side_revenue,
    trading_metrics.secondary_supply_side_revenue,
    trading_metrics.total_supply_side_revenue,
    trading_metrics.protocol_revenue,
    trading_metrics.operating_expenses,
    trading_metrics.token_incentives,
    trading_metrics.protocol_earnings,
   -- all_tvl.tvl_usd as tvl,
    tvl_balancer_v1.tvl_usd,
    treasury.net_treasury_usd as treasury_value,
    net_treasury.net_treasury_usd as net_treasury_value,
    treasury_native.treasury_native as treasury_native,
    trading_metrics.trading_volume,
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
left join tvl_balancer_v1 using (date)
left join trading_metrics using (date)