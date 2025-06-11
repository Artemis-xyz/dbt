{{
    config(
        materialized="table",
        snowflake_warehouse="AEVO",
        database="aevo",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_aevo_trading_volume") }}
        group by date
    )
    , price as ({{ get_coingecko_metrics("aevo-exchange") }})
select
    date
    , 'aevo' as app
    , 'DeFi' as category
    , trading_volume
    -- standardize metrics
    , trading_volume as perp_volume
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
from trading_volume_data
left join price using(date)
where date < to_date(sysdate())
