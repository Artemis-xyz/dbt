{{
    config(
        materialized="table",
        snowflake_warehouse="RONIN",
        database="ronin",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    ronin_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_ronin_daily_dex_volumes") }}
    ),
    price_data as ({{ get_coingecko_metrics("ronin") }})
select
    ronin_dex_volumes.date
    , dex_volumes
    , adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating
    -- Chain Usage Metrics
    , dex_volumes AS chain_spot_volume
from ronin_dex_volumes   
left join price_data on ronin_dex_volumes.date = price_data.date
where ronin_dex_volumes.date < to_date(sysdate())
