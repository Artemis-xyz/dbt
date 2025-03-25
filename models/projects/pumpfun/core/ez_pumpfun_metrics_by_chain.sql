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

swap_metrics AS (
    SELECT 
        date, 
        COUNT(DISTINCT tx_id) AS number_of_swaps, 
        COUNT(DISTINCT trader) AS unique_traders, 
        SUM(amount) AS trading_volume, 
        AVG(amount) AS average_traded_volume 
    FROM trades 
    WHERE date < CURRENT_DATE() --exclude today's data to account for data lag
    GROUP BY 1
)

select
    date_spine.date,
    'solana' as chain,
    coalesce(swap_metrics.unique_traders, 0) as unique_traders,
    coalesce(swap_metrics.number_of_swaps, 0) as number_of_swaps,
    coalesce(swap_metrics.trading_volume, 0) as trading_volume
from date_spine
left join swap_metrics using(date)
order by date desc