{{
    config(
        materialized="table",
        snowflake_warehouse="APEX",
        database="apex",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, coalesce(trading_volume, 0) as trading_volume, chain
        from {{ ref("fact_apex_trading_volume") }}
    )
    , market_metrics as ({{ get_coingecko_metrics("apex-token-2") }})
select
    date
    , 'apex' as artemis_id
    , chain

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , trading_volume as perp_volume

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

from trading_volume_data
left join market_metrics using(date)
where date < to_date(sysdate())
