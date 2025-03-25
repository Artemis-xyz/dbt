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

swap_metrics AS (
    SELECT 
        date, 
        COUNT(DISTINCT tx_id) AS number_of_swaps, 
        COUNT(DISTINCT trader) AS unique_traders, 
        SUM(amount) AS trading_volume_sol, 
        SUM(amount_usd) AS trading_volume_usd, 
        AVG(amount_usd) AS average_traded_volume_usd 
    FROM trades
    WHERE date < CURRENT_DATE() --exclude today's data to account for data lag
    GROUP BY 1
)

select
    date_spine.date,
    coalesce(swap_metrics.unique_traders, 0) as unique_traders,
    coalesce(swap_metrics.number_of_swaps, 0) as number_of_swaps,
    coalesce(swap_metrics.trading_volume_sol, 0) as trading_volume_sol,
    coalesce(swap_metrics.trading_volume_usd, 0) as trading_volume_usd,
from date_spine
left join swap_metrics using(date)
order by date desc