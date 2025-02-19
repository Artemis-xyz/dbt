{{
    config(
        materialized='table',
        snowflake_warehouse='USUAL',
        database='usual',
        schema='core',
        alias='ez_metrics'
    )
}}

with usd0_metrics as (
    select
        date,
        stablecoin_txns,
        stablecoin_dau,
        stablecoin_total_supply as usd0_tvl
    from {{ ref('ez_usd0_metrics') }}
), 

usd0pp_metrics as (
    select
        date,
        usd0pp_tvl as usd0pp_tvl
    from {{ ref('fact_usd0pp_tvl') }}
), 

usual_fees as (
    select 
        date,
        daily_treasury_revenue,
        cumulative_treasury_revenue,
        fees, 
        cumulative_fees 
    from {{ ref('fact_usual_fees') }}
), 

usual_burn_mint as (
    select 
        date, 
        daily_supply, 
        cumulative_supply, 
        daily_treasury, 
        cumulative_treasury, 
        daily_burned, 
        cumulative_burned 
    from {{ ref('fact_usual_burn_mint') }}
)
select 
    usd0.date,
    usd0.stablecoin_txns as stablecoin_txns,
    usd0.stablecoin_dau as stablecoin_dau,
    usd0.usd0_tvl as usd0_tvl,
    usd0pp.usd0pp_tvl as usd0pp_tvl,
    usual.fees as fees,
    usual.daily_treasury_revenue as treasury_revenue,
    ubm.daily_supply as daily_supply,
    ubm.daily_treasury as daily_treasury,
    ubm.daily_burned as daily_burned,
    -- revenue is the sum of treasury revenue, daily burned, and fees
    usual.fees + usual.daily_treasury_revenue + ubm.daily_burned as revenue
from usd0_metrics usd0 
left join usd0pp_metrics usd0pp on usd0.date = usd0pp.date
left join usual_fees usual on usd0.date = usual.date
left join usual_burn_mint ubm on usd0.date = ubm.date
