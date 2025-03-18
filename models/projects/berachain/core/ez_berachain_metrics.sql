{{
    config(
        materialized="table",
        snowflake_warehouse="BERACHAIN",
        database="berachain",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
     price_data as ({{ get_coingecko_metrics('berachain-bera') }}),
     dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_berachain_daily_dex_volumes") }}
     ),
     supply_data as (
        select 
            date
            , premine_unlocks_native
            , emission_native
            , burns_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref('fact_berachain_supply_data') }}
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
    , premine_unlocks_native
    , emission_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
from {{ ref("fact_berachain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join dex_volumes on f.date = dex_volumes.date
left join supply_data on f.date = supply_data.date
where f.date  < to_date(sysdate())