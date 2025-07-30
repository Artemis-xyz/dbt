-- Deprecated 7/15/2025
{{
    config(
        materialized="table",
        snowflake_warehouse="VERTEX",
        database="vertex",
        schema="core",
        alias="ez_metrics_by_chain",
        enabled=false
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
    , 'vertex' as artemis_id
    , chain

    --Usage Data
    , unique_traders as perp_dau
    , unique_traders as dau
    , trading_volume as perp_volume

    --Financial Statements
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
from unique_traders_data
left join trading_volume_data using(date, chain)
left join token_incentives using(date)
where date < to_date(sysdate())
