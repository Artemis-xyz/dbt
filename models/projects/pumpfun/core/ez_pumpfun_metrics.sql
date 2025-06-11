{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='core',
        alias='ez_metrics',
    )
 }}

with date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-10-01' and (to_date(sysdate()) - 1)
),

trades as (
    select *
    from {{ ref('fact_pumpfun_trades') }}
),
swap_metrics as (
    select 
        date,
        'pump.fun' as version, 
        COUNT(distinct tx_id) as number_of_swaps, 
        COUNT(distinct trader) as unique_traders, 
        SUM(amount) as trading_volume_sol, 
        SUM(amount_usd_artemis) as trading_volume_usd, 
        AVG(amount_usd_artemis) as average_traded_volume_usd 
    from trades
    where date > '2024-05-31'
    group by 1, 2
),
daily_revenues as (
    select
        date,
        'pump.fun' as version,
        fees as launchpad_fees
    from {{ ref('fact_pumpfun_dailyrevenues') }}
    where date > '2024-05-31'
),
pumpswap_metrics as (
    select
        date,
        'pumpswap' as version,
        spot_dau,
        spot_txns,
        spot_volume,
        spot_fees
    from {{ ref('fact_pumpswap_metrics') }}
    where date >= '2025-03-20'
)

-- Final combined query with one row per day
select
    date_spine.date,
    --Standardized Metrics
    coalesce(swap_metrics.unique_traders, 0) as launchpad_dau,
    coalesce(pumpswap_metrics.spot_dau, 0) as spot_dau,
    coalesce(swap_metrics.number_of_swaps, 0) as launchpad_txns,
    coalesce(pumpswap_metrics.spot_txns, 0) as spot_txns,
    coalesce(swap_metrics.trading_volume_usd, 0) as launchpad_volume,
    coalesce(pumpswap_metrics.spot_volume, 0) as spot_volume,
    coalesce(daily_revenues.launchpad_fees, 0) as launchpad_fees,
    coalesce(pumpswap_metrics.spot_fees, 0) as spot_fees,
    coalesce(daily_revenues.launchpad_fees, 0) + coalesce(pumpswap_metrics.spot_fees, 0) as ecosystem_revenue
from date_spine
left join swap_metrics using(date)
left join daily_revenues using(date)
left join pumpswap_metrics using(date)
where date_spine.date < to_date(sysdate())
order by date desc