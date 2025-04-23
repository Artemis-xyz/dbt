{{
    config(
        materialized="table",
        snowflake_warehouse="KAIA",
        database="kaia",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    kaia_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_kaia_daily_dex_volumes") }}
    ), 
    price_data as ({{ get_coingecko_metrics("kaia") }})
select
    date
    , kaia_dex_volumes.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Metrics
    , kaia_dex_volumes.dex_volumes AS chain_spot_volume
from kaia_dex_volumes   
left join price_data using (date)
where date < to_date(sysdate())