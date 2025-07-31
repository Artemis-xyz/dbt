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
)
, virtuals_volume as (
    select
        date
        , coalesce(volume_native, 0) as volume_native
        , coalesce(volume_usd, 0) as trading_volume
    from {{ ref("fact_virtuals_volume") }}
)
select
    date_spine.date
    , 'base' as chain

    -- Standardized Metrics

    -- AI Metrics
    , virtuals_volume.trading_volume as ai_volume

from date_spine
left join virtuals_volume using (date)