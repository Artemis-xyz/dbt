{{
    config(
        materialized="table",
        snowflake_warehouse="BOBA",
        database="boba",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with 
    boba_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes
        from {{ ref("fact_boba_daily_dex_volumes") }}
    )
select
    date
    , 'boba' as artemis_id
    , 'boba' as chain

    -- Standardized Metrics

    -- Usage Data
    , dex_volumes as chain_spot_volume

from boba_dex_volumes
where date < to_date(sysdate())
