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
        daily_treasury_revenue as collateral_yield,
        cumulative_treasury_revenue as cumulative_collateral_yield,
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
        circulating_supply_native,
        cumulative_supply
    from {{ ref('fact_usual_burn_mint') }}
)

, market_metrics as (
    ({{ get_coingecko_metrics("usual") }}) 
)

select 
    usd0.date
    , usd0.usd0_tvl
    , usd0pp.usd0pp_tvl
    , usual.fees
    , usual.collateral_yield
    , ubm.daily_supply
    , ubm.daily_treasury
    , ubm.daily_burned
    -- revenue is the sum of treasury revenue, daily burned, and fees
    , usual.fees + (usual.collateral_yield * mm.price) + (ubm.daily_burned * mm.price) as revenue

    -- Standardized Metrics
    , usd0.usd0_tvl + usd0pp.usd0pp_tvl as tvl
    , usd0.stablecoin_txns
    , usd0.stablecoin_dau

    -- Revenue Metrics
    , (usual.collateral_yield * mm.price) + ubm.daily_treasury as treasury_cash_flow
    , ubm.daily_burned as burned_cash_flow_native
    , ubm.daily_burned * mm.price as burned_cash_flow
    , usual.fees + ubm.daily_treasury + (usual.collateral_yield * mm.price) + (ubm.daily_burned * mm.price) as gross_protocol_revenue
    , usual.fees + (ubm.daily_burned * mm.price) as usual_issuance_module
    -- 10% of issuance module goes to USUAL* holders
    , usual_issuance_module * 0.1 as fee_sharing_token_cash_flow
    -- 90% of issuance module goes to protocol's operations, stakers, LPs and ecosystem
    , usual_issuance_module * 0.9 as service_cash_flow

    , ubm.circulating_supply_native

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