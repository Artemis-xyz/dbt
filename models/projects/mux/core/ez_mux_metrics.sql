{{
    config(
        materialized="table",
        snowflake_warehouse="MUX",
        database="mux",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    mux_data as (
        select 
            date
            , sum(trading_volume) as trading_volume
            , sum(unique_traders) as unique_traders
        from {{ ref("fact_mux_trading_volume_unique_traders") }}
        where chain is not null
        group by 1
    )
    , price as ({{ get_coingecko_metrics("mcdex") }})
select
    date
    , 'mux' as app
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
from mux_data
left join price using(date)
where date < to_date(sysdate())
