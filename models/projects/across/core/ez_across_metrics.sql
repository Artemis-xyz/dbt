{{
    config(
        materialized="table",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_across_bridge_volume") }}
        where chain is null
    ),
    bridge_daa as (
        select date, bridge_daa
        from {{ ref("fact_across_bridge_daa") }}
    )
select
    bridge_volume.date as date,
    'across' as app,
    'Bridge' as category,
    bridge_volume.bridge_volume,
    bridge_daa.bridge_daa
from bridge_volume
left join bridge_daa on bridge_volume.date = bridge_daa.date
where bridge_volume.date < to_date(sysdate())
