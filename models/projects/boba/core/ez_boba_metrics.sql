{{
    config(
        materialized="table",
        snowflake_warehouse="BOBA",
        database="boba",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    boba_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_boba_daily_dex_volumes") }}
    ),
    price_data as ({{ get_coingecko_metrics('boba-network') }})
select
    d.date
    , dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , dex_volumes as chain_dex_volumes
    , token_turnover_circulating
    , token_turnover_fdv
from boba_dex_volumes d
left join price_data using(d.date)
where d.date < to_date(sysdate())
