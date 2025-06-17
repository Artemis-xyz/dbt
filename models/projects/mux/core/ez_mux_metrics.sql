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
    , token_incentives as (
        select
            date,
            sum(token_incentives) as token_incentives
        from {{ ref("fact_mux_token_incentives") }}
        group by 1
    )
select
    date
    , 'mux' as app
    , 'DeFi' as category

    -- Usage Metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau

    -- Market Data
    , price.price
    , price.market_cap
    , price.fdmc
    , price.token_turnover_circulating
    , price.token_turnover_fdv
    , price.token_volume

    -- Cashflow Metrics
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

from mux_data
left join price using(date)
left join token_incentives using(date)
where date < to_date(sysdate())
