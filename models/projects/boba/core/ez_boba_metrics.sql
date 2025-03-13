{{
    config(
        materialized="table",
        snowflake_warehouse="BOBA",
        database="boba",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    boba_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_boba_daily_dex_volumes") }}
    )
select
    date,
    dex_volumes
from boba_dex_volumes   
where date < to_date(sysdate())
