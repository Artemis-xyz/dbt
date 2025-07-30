{{
    config(
        materialized="table",
        snowflake_warehouse="CORN",
        database="corn",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
with corn_dex_volumes as (
    select date, chain, coalesce(daily_volume, 0) as dex_volumes
        from {{ ref("fact_corn_daily_dex_volumes") }}
)

select
    corn_dex_volumes.date
    , 'corn' as artemis_id
    , corn_dex_volumes.chain
    
    -- Standardized Metrics

    -- Usage Data
    , corn_dex_volumes.dex_volumes as spot_volume
    , corn_dex_volumes.adjusted_dex_volumes

from corn_dex_volumes
left join corn_market_data cmd using (date)