{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
        database="nova",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with nova_dex_volumes as (
    select date, coalesce(daily_volume, 0) as dex_volumes
    from {{ ref("fact_nova_daily_dex_volumes") }}
)

select
    nova_dex_volumes.date
    , 'nova' as artemis_id
    , 'solana' as chain

    -- Standardized Metrics

    -- Usage Data
    , nova_dex_volumes.dex_volumes as spot_volume

from nova_dex_volumes   
where nova_dex_volumes.date < to_date(sysdate())
