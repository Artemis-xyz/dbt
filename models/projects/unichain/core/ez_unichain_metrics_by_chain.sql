{{
    config(
        materialized="table",
        snowflake_warehouse="UNICHAIN",
        database="unichain",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with 
     price_data as ({{ get_coingecko_metrics('uniswap') }})
     , unichain_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_unichain_daily_dex_volumes") }}
    )
select
    f.date
    , 'unichain' as chain
    , dune_dex_volumes_unichain.dex_volumes
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
    , cost as l1_cash_flow
    , cost_native as l1_cash_flow_native
    , revenue as foundation_cash_flow
    , revenue_native as foundation_cash_flow_native
    , token_turnover_circulating
    , token_turnover_fdv
from {{ ref("fact_unichain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join unichain_dex_volumes as dune_dex_volumes_unichain on f.date = dune_dex_volumes_unichain.date
where f.date < to_date(sysdate())