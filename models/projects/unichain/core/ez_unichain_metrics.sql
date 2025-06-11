{{
    config(
        materialized="table",
        snowflake_warehouse="UNICHAIN",
        database="unichain",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
     price_data as ({{ get_coingecko_metrics('uniswap') }})
     , unichain_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_unichain_daily_dex_volumes") }}
    )
select
    f.date
    , dune_dex_volumes_unichain.dex_volumes
    , dune_dex_volumes_unichain.adjusted_dex_volumes
    -- Old Metrics Needed For Compatibility
    , txns
    , daa as dau
    , fees
    , fees_native
    , cost
    , cost_native
    , revenue
    , revenue_native

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    , dune_dex_volumes_unichain.dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_fee_allocation
    , cost_native as l1_fee_allocation_native
    , revenue as foundation_fee_allocation
    , revenue_native as foundation_fee_allocation_native
    , token_turnover_circulating
    , token_turnover_fdv
from {{ ref("fact_unichain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join unichain_dex_volumes as dune_dex_volumes_unichain on f.date = dune_dex_volumes_unichain.date
where f.date < to_date(sysdate())
