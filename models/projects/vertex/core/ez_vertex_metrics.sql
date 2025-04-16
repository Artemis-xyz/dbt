{{
    config(
        materialized="table",
        snowflake_warehouse="VERTEX",
        database="vertex",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_vertex_trading_volume") }}
        group by date
    )
    , unique_traders_data as (
        select date, sum(unique_traders) as unique_traders
        from {{ ref("fact_vertex_unique_traders") }}
        group by date
    )
    , price as ({{ get_coingecko_metrics("vertex-protocol") }})
select
    date
    , 'vertex' as app
    , 'DeFi' as category
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
from trading_volume_data
left join unique_traders_data using(date)
left join price using(date)
where date < to_date(sysdate())
