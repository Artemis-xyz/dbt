{{
    config(
        materialized='table',
        snowflake_warehouse='USUAL',
        database='usual',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

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
        , usd0pp_tvl
    from {{ ref('fact_usd0pp_tvl') }}
) 

, usual_fees as (
    select 
        date
        , daily_treasury_revenue
        , cumulative_treasury_revenue
        , fees
        , cumulative_fees 
    from {{ ref('fact_usual_fees') }}
) 

, usual_burn_mint as (
    select 
        date
        , daily_supply
        , cumulative_supply
        , daily_treasury
        , cumulative_treasury
        , daily_burned
        , cumulative_burned 
    from {{ ref('fact_usual_burn_mint') }}
)
select 
    usd0.date
    , chain as 'base'
    , usd0.stablecoin_txns as stablecoin_txns
    , usd0.stablecoin_dau as stablecoin_dau
    , usd0.usd0_tvl as usd0_tvl
    , usd0pp.usd0pp_tvl as usd0pp_tvl
    , usual.fees as fees
    , usual.daily_treasury_revenue as treasury_revenue
    , ubm.daily_supply as daily_supply
    , ubm.daily_treasury as daily_treasury
    , ubm.daily_burned as daily_burned
    -- revenue is the sum of treasury revenue, daily burned, and fees
    , usual.fees + usual.daily_treasury_revenue + ubm.daily_burned as revenue

    -- Standardized Metrics

    -- Stablecoin Metrics
    , coalesce(market_data.stablecoin_dau, 0) as stablecoin_dau
    , coalesce(market_data.stablecoin_txns, 0) as stablecoin_txns
    , coalesce(usd0.usd0_tvl, 0) as stablecoin_total_supply
    , coalesce(market_data.stablecoin_volume, 0) as stablecoin_volume

    -- Crypto metrics
    , coalesce(usd0pp.usd0pp_tvl, 0) as tvl
    , coalesce(usd0pp.usd0pp_tvl, 0) - coalesce(lag(usd0pp.usd0pp_tvl) over (order by usd0pp.date), 0) as tvl_change

    -- Protocol Metrics
    , coalesce(ubm.daily_treasury, 0) as treasury

    -- Cash Flow Metrics
    , coalesce(usual.fees, 0) + coalesce(usual.daily_treasury_revenue, 0) as gross_protocol_revenue
    , coalesce(usual.daily_treasury_revenue, 0) as treasury_cash_flow
    , coalesce(ubm.daily_burned, 0) as burned_cash_flow_native

    -- Supply Metrics
    , coalesce(ubm.daily_supply, 0) as mints_native
from usd0_metrics usd0 
left join usd0pp_metrics usd0pp on usd0.date = usd0pp.date
left join usual_fees usual on usd0.date = usual.date
left join usual_burn_mint ubm on usd0.date = ubm.date
