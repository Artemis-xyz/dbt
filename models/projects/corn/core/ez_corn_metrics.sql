{{
    config(
        materialized="table",
        snowflake_warehouse="CORN",
        database="corn",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    corn_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_corn_daily_dex_volumes") }}
    )

select
    date
    , dex_volumes
from corn_dex_volumes