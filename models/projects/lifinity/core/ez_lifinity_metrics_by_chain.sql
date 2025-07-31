{{
    config(
        materialized="table",
        snowflake_warehouse="LIFINITY",
        database="lifinity",
        schema="core",
        alias="ez_metrics_by_chain"
    )
}}

with 
    lifinity_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes
        from {{ ref("fact_lifinity_dex_volumes") }}
    )
select
    date
    , 'lifinity' as artemis_id
    , 'solana' as chain

    -- Standardized Metrics

    -- Usage Data
    , dex_volumes as spot_volume

from lifinity_dex_volumes   
where date < to_date(sysdate())
