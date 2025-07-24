{{
    config(
        materialized='incremental',
        snowflake_warehouse='USUAL',
        database='usual',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var('backfill_columns', []),
        merge_exclude_columns=['created_on'] | reject('in', var('backfill_columns', [])) | list,
        full_refresh=false,
        tags=['ez_metrics']
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with usd0_metrics as (
    select
        date
        , stablecoin_txns
        , stablecoin_dau
        , stablecoin_total_supply as usd0_tvl
    from {{ ref('ez_usd0_metrics') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)

, usd0pp_metrics as (
    select
        date
        , usd0pp_tvl as usd0pp_tvl
    from {{ ref('fact_usd0pp_tvl') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)

, usual_fees as (
    select 
        date
        , daily_treasury_revenue as collateral_yield
        , cumulative_treasury_revenue as cumulative_collateral_yield
        , fees 
        , cumulative_fees
        , usualx_unstake_fees_daily
        , treasury_fee
    from {{ ref('fact_usual_fees') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
) 

, usual_burn_mint as (
    select 
        date
        , gross_emissions_native
        , burns_native
        , net_supply_change_native
        , circulating_supply_native
        , daily_treasury
        , daily_treasury_usualstar
        , daily_treasury_usualx
    from {{ ref('fact_usual_burn_mint') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
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
    , ubm.daily_treasury
    -- revenue is the sum of treasury revenue, daily burned, and fees
    , usual.fees + (usual.collateral_yield * mm.price) + (ubm.burns_native * mm.price) as revenue

    -- Standardized Metrics
    , usd0.usd0_tvl + usd0pp.usd0pp_tvl as tvl
    , usd0.stablecoin_txns
    , usd0.stablecoin_dau

    -- Revenue Metrics
    , (usual.collateral_yield * mm.price) as yield_generated
    , (ubm.burns_native * mm.price) as burned_fee_allocation
    , ubm.burns_native as burned_fee_allocation_native
    -- Gross Protocol Revenue
    , (usual.usualx_unstake_fees_daily) + (usual.treasury_fee) + (yield_generated) + (burned_fee_allocation) + (ubm.daily_treasury_usualstar * mm.price) + (ubm.daily_treasury_usualx * mm.price) as ecosystem_revenue
    -- Cash Flow Buckets
    , (usual.collateral_yield * mm.price) + (usual.treasury_fee) as treasury_fee_allocation
    , (usual.usualx_unstake_fees_daily) + (ubm.daily_treasury_usualstar * mm.price) + (ubm.daily_treasury_usualx * mm.price) as staking_fee_allocation

    -- Supply Metrics
    , ubm.gross_emissions_native
    , ubm.burns_native
    , ubm.net_supply_change_native
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
{{ ez_metrics_incremental('usd0.date', backfill_date) }}
and usd0.date < to_date(sysdate())