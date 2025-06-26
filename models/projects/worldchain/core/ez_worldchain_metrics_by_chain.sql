{{
    config(
        materialized="table",
        snowflake_warehouse="WORLDCHAIN",
        database="worldchain",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with 
     price_data as ({{ get_coingecko_metrics('worldcoin-wld') }})
    , worldchain_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_worldchain_daily_dex_volumes") }}
    )
select
    f.date
    , 'worldchain' as chain
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , daa as chain_dau
    , txns as chain_txns
    , worldchain_dex_volumes.dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as fees
    , fees_native as fees_native
    , cost as l1_fee_allocation
    , cost_native as l1_fee_allocation_native
    , revenue as foundation_fee_allocation
    , revenue_native as foundation_fee_allocation_native
    , token_turnover_circulating
    , token_turnover_fdv
from {{ ref("fact_worldchain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join worldchain_dex_volumes on f.date = worldchain_dex_volumes.date
where f.date  < to_date(sysdate())
