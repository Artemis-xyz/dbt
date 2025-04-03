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
)

, usd0pp_metrics as (
    select
        date,
        usd0pp_tvl as usd0pp_tvl
    from {{ ref('fact_usd0pp_tvl') }}
)

, usual_fees as (
    select 
        date,
        daily_treasury_revenue,
        cumulative_treasury_revenue,
        fees, 
        cumulative_fees 
    from {{ ref('fact_usual_fees') }}
) 

, usual_burn_mint as (
    select 
        date, 
        daily_supply, 
        cumulative_supply, 
        daily_treasury, 
        cumulative_treasury, 
        daily_burned, 
        cumulative_burned,
        current_circulating_supply_native,
        cumulative_supply
    from {{ ref('fact_usual_burn_mint') }}
)

, market_metrics as (
    select * from ({{ get_coingecko_price_with_latest("usual") }}) 
)

select 
    usd0.date
    , usd0.stablecoin_txns
    , usd0.stablecoin_dau
    , usd0.usd0_tvl
    , usd0pp.usd0pp_tvl
    , usual.fees
    , usual.daily_treasury_revenue as treasury_revenue
    , ubm.daily_supply
    , ubm.daily_treasury
    , ubm.daily_burned
    -- revenue is the sum of treasury revenue, daily burned, and fees
    , usual.fees + (usual.daily_treasury_revenue * mm.price) + (ubm.daily_burned * mm.price) as revenue

    -- Standardized Metrics
    , usd0.stablecoin_txns
    , usd0.stablecoin_dau

    -- Revenue Metrics
    , usual.fees + (usual.daily_treasury_revenue * mm.price) + (ubm.daily_burned * mm.price) as gross_protocol_revenue
    , usual.daily_treasury * mm.price as treasury_cash_flow
    , ubm.daily_burned as burned_cash_flow_native

    , ubm.current_circulating_supply_native

    -- Market Metrics
    , mm.price
    , mm.token_volume
    , mm.market_cap
    , mm.fdmc
    , mm.token_turnover_circulating
    , mm.token_turnover_fdv

from usd0_metrics usd0 
left join usd0pp_metrics usd0pp on usd0.date = usd0pp.date
left join usual_fees usual on usd0.date = usual.date
left join usual_burn_mint ubm on usd0.date = ubm.date
left join market_metrics mm on usd0.date = mm.date