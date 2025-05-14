{{
    config(
        materialized="table",
        snowflake_warehouse="WORLDCHAIN",
        database="worldchain",
        schema="core",
        alias="ez_metrics",
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
    , txns
    , daa as dau
    , fees_native
    , fees
    , cost
    , cost_native
    , revenue
    , revenue_native
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , dau as chain_dau
    , txns as chain_txns
    , dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_cash_flow
    , cost_native as l1_cash_flow_native
    , revenue as foundation_cash_flow
    , revenue_native as foundation_cash_flow_native
    , token_turnover_circulating
    , token_turnover_fdv
from {{ ref("fact_worldchain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join worldchain_dex_volumes on f.date = worldchain_dex_volumes.date
where f.date  < to_date(sysdate())
