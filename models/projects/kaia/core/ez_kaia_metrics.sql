{{
    config(
        materialized="table",
        snowflake_warehouse="KAIA",
        database="kaia",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    kaia_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_kaia_daily_dex_volumes") }}
    )
select
    date,
    dex_volumes
from kaia_dex_volumes   
where date < to_date(sysdate())
