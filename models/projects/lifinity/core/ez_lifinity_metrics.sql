{{
    config(
        materialized="table",
        snowflake_warehouse="LIFINITY",
        database="lifinity",
        schema="core",
        alias="ez_metrics"
    )
}}

with lifinity_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_lifinity_dex_volumes") }}
)
, lifinity_market_data as (
    {{ get_coingecko_metrics('lifinity') }}
)

select
    date
    , dex_volumes

    -- Standardized Metrics
    , dex_volumes as spot_volume

    -- Market Metrics
    , lmd.price
    , lmd.market_cap
    , lmd.fdmc
    , lmd.token_turnover_circulating
    , lmd.token_turnover_fdv
    , lmd.token_volume

from lifinity_dex_volumes   
left join lifinity_market_data lmd using (date)
where date < to_date(sysdate())
