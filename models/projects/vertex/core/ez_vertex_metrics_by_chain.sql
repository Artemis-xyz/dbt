{{
    config(
        materialized="table",
        snowflake_warehouse="VERTEX",
        database="vertex",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_vertex_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_vertex_unique_traders") }}
    )
    , token_incentives as (
        select
            date,
            sum(amount) as token_incentives_native,
            sum(amount_usd) as token_incentives
        from {{ ref("fact_vertex_token_incentives") }}
        group by date
    )
select
    date
    , 'vertex' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    , token_incentives
from unique_traders_data
left join trading_volume_data using(date, chain)
left join token_incentives using(date)
where date < to_date(sysdate())
