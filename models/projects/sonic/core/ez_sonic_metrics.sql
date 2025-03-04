{{
    config(
        materialized="table",
        snowflake_warehouse="SONIC",
        database="sonic",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    sonic_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_sonic_daily_dex_volumes") }}
    )
select
    date,
    dex_volumes
from sonic_dex_volumes   
where date < to_date(sysdate())
