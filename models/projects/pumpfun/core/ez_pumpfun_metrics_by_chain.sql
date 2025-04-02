{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='core',
        alias='ez_metrics_by_chain',
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

daily_revenues as (
    select *
    from {{ ref('fact_pumpfun_dailyrevenues') }}
    where date > '2024-05-31'
),

swap_metrics as (
    select 
        date, 
        count(distinct tx_id) as number_of_swaps, 
        count(distinct trader) as unique_traders, 
        sum(amount_usd_artemis) as trading_volume, 
        avg(amount_usd_artemis) as average_traded_volume 
    from trades
    where date > '2024-05-31'
    group by 1
),

pumpswap_metrics as (
    select
        date,
        spot_dau as unique_traders,
        spot_txns as number_of_swaps,
        trading_volume_usd as trading_volume,
        daily_lp_fees_usd + daily_protocol_fees_usd as spot_fees
    from {{ ref('fact_pumpswap_metrics') }}
    where date >= '2025-03-20'
)

select
    date_spine.date,
    'solana' as chain,
    --Standardized Metrics
    coalesce(swap_metrics.unique_traders, 0) as launchpad_dau,
    coalesce(pumpswap_metrics.unique_traders, 0) as spot_dau,
    coalesce(swap_metrics.number_of_swaps, 0) as launchpad_txns,
    coalesce(pumpswap_metrics.number_of_swaps, 0) as spot_txns,
    coalesce(swap_metrics.trading_volume, 0) as launchpad_volume,
    coalesce(pumpswap_metrics.trading_volume, 0) as spot_volume,
    coalesce(daily_revenues.fees, 0) as launchpad_fees,
    coalesce(pumpswap_metrics.spot_fees, 0) as spot_fees,
    launchpad_fees + spot_fees as gross_protocol_revenue
from date_spine
left join swap_metrics using(date)
left join daily_revenues using(date)
left join pumpswap_metrics using(date)
where date_spine.date < to_date(sysdate()) - 1
order by date desc