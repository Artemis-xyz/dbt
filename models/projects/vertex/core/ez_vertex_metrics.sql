-- Deprecated 7/15/2025
{{
    config(
        materialized="table",
        snowflake_warehouse="VERTEX",
        database="vertex",
        schema="core",
        alias="ez_metrics",
    )
}}

with trading_volume_data as (
    select date, sum(trading_volume) as trading_volume
    from {{ ref("fact_vertex_trading_volume") }}
    group by date
)
, unique_traders_data as (
    select date, sum(unique_traders) as unique_traders
    from {{ ref("fact_vertex_unique_traders") }}
    group by date
)
, token_incentives as (
    select 
        date,
        sum(amount) as token_incentives_native,
        sum(amount_usd) as token_incentives
    from {{ ref("fact_vertex_token_incentives") }}
    group by date
)
, date_spine as (
    select date_spine.date
    from {{ ref('dim_date_spine') }} date_spine
    where date_spine.date between '2023-11-01' and to_date(sysdate())
)
, market_metrics as ({{ get_coingecko_metrics("vertex-protocol") }})

select
    date_spine.date
    , 'vertex' as app
    , 'DeFi' as category

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    --Usage Metrics
    , trading_volume_data.trading_volume as perp_volume
    , unique_traders_data.unique_traders as perp_dau

    -- Cashflow Incentives
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

from date_spine
left join market_metrics using(date)
left join trading_volume_data using(date)
left join unique_traders_data using(date)
left join token_incentives using(date)
where date_spine.date < to_date(sysdate())
