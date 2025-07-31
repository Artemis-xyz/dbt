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
        merge_exclude_columns=['created_on'] if not var('backfill_columns', []) else none,
        full_refresh=var("full_refresh", false),
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
)

, usd0pp_metrics as (
    select
        date
        , usd0pp_tvl as usd0pp_tvl
    from {{ ref('fact_usd0pp_tvl') }}
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
)

, market_metrics as (
    ({{ get_coingecko_metrics("usual") }}) 
)

select 
    usd0_metrics.date
    , 'usual' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.token_volume
    , market_metrics.market_cap
    , market_metrics.fdmc

    -- Usage Data
    , usd0_metrics.stablecoin_txns
    , usd0_metrics.stablecoin_dau
    , usd0_metrics.usd0_tvl
    , usd0pp_metrics.usd0pp_tvl
    , usual_fees.collateral_yield
    , usd0_metrics.usd0_tvl + usd0pp_metrics.usd0pp_tvl as tvl

    -- Fee Data
    , (usual_fees.usualx_unstake_fees_daily) + (usual_fees.treasury_fee) + (yield_generated) + (burned_fee_allocation) + (usual_burn_mint.daily_treasury_usualstar * market_metrics.price) + (usual_burn_mint.daily_treasury_usualx * market_metrics.price) as fees
    , (usual_fees.collateral_yield * market_metrics.price) + (usual_fees.treasury_fee) as treasury_fee_allocation
    , usual_burn_mint.burns_native as burned_fee_allocation_native
    , (usual_burn_mint.burns_native * market_metrics.price) as burned_fee_allocation
    , (usual_fees.collateral_yield * market_metrics.price) as yield_generated
    , (usual_fees.usualx_unstake_fees_daily) + (usual_burn_mint.daily_treasury_usualstar * market_metrics.price) + (usual_burn_mint.daily_treasury_usualx * market_metrics.price) as staking_fee_allocation

    -- Financial Statement
    , usual_fees.fees + (usual_fees.collateral_yield * market_metrics.price) + (usual_burn_mint.burns_native * market_metrics.price) as revenue -- revenue is the sum of fees, collateral yield, and burns

    -- Stablecoin Data
    , usd0_metrics.stablecoin_txns
    , usd0_metrics.stablecoin_dau

    -- Supply Data
    , supply_data.gross_emissions_native
    , supply_data.burns_native
    , supply_data.circulating_supply_native

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Treasury Data
    , usual_burn_mint.daily_treasury as treasury

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from usd0_metrics 
left join usd0pp_metrics on usd0_metrics.date = usd0pp_metrics.date
left join usual_fees on usd0_metrics.date = usual_fees.date
left join usual_burn_mint on usd0_metrics.date = usual_burn_mint.date
left join market_metrics on usd0_metrics.date = market_metrics.date
where true
{{ ez_metrics_incremental('usd0_metrics.date', backfill_date) }}
and usd0_metrics.date < to_date(sysdate())