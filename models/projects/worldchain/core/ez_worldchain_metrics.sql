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
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    , dex_volumes
from {{ ref("fact_worldchain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join worldchain_dex_volumes on f.date = worldchain_dex_volumes.date
where f.date  < to_date(sysdate())
