{{
    config(
        materialized="table",
        snowflake_warehouse="RONIN",
        database="ronin",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    ronin_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_ronin_daily_dex_volumes") }}
    )
select
    date,
    dex_volumes
from ronin_dex_volumes   
where date < to_date(sysdate())
