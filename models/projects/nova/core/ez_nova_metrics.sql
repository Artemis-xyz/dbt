{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
        database="nova",
        schema="core",
        alias="ez_metrics",
    )
}}

with nova_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_nova_daily_dex_volumes") }}
)
, nova_market_data as (
    {{ get_coingecko_metrics('novadex') }}
)
select
    date
    , dex_volumes

    -- Standardized Metrics
    , dex_volumes as spot_volume

    -- Market Metrics
    , nmd.price
    , nmd.market_cap
    , nmd.fdmc
    , nmd.token_turnover_circulating
    , nmd.token_turnover_fdv
    , nmd.token_volume
from nova_dex_volumes   
left join nova_market_data nmd using (date)
where date < to_date(sysdate())
