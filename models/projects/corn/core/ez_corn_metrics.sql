{{
    config(
        materialized="table",
        snowflake_warehouse="CORN",
        database="corn",
        schema="core",
        alias="ez_metrics",
    )
}}
with corn_dex_volumes as (
    select date, daily_volume as dex_volumes
        from {{ ref("fact_corn_daily_dex_volumes") }}
)
, corn_market_data as (
    {{ get_coingecko_metrics('corn-3') }}
)

select
    date
    , dex_volumes

    -- Standardized Metrics
    , dex_volumes as spot_volume

    -- Market Metrics
    , cmd.price
    , cmd.market_cap
    , cmd.fdmc
    , cmd.token_turnover_circulating
    , cmd.token_turnover_fdv
    , cmd.token_volume
from corn_dex_volumes
left join corn_market_data cmd using (date)