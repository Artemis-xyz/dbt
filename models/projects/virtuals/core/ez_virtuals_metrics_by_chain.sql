{{
    config(
        materialized="table",
        snowflake_warehouse = 'VIRTUALS',
        database = 'VIRTUALS',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2024-09-10' and to_date(sysdate())
),
volume as (
    select
        date
        , volume_native
        , volume_usd as trading_volume
    from {{ ref("fact_virtuals_volume") }}
)
select
    ds.date
    , 'base' as chain
    , coalesce(v.volume_native, 0) as volume_native
    , coalesce(v.trading_volume, 0) as trading_volume

    -- Standardized Metrics

    -- AI Metrics
    , coalesce(v.trading_volume, 0) as ai_volume
from volume v
left join date_spine ds on v.date = ds.date