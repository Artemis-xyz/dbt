{{
    config(
        materialized="table",
        snowflake_warehouse="LIFINITY",
        database="lifinity",
        schema="core",
        alias="ez_metrics"
    )
}}

with 
    lifinity_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_lifinity_dex_volumes") }}
    )
select
    date,
    dex_volumes
from lifinity_dex_volumes   
where date < to_date(sysdate())
