{{
    config(
        materialized="table",
        snowflake_warehouse="RAINBOW_BRIDGE",
        database="rainbow_bridge",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_rainbow_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_dau as (
        select date, bridge_dau
        from {{ ref("fact_rainbow_bridge_bridge_dau") }}
    )
select
    bridge_volume.date as date,
    'rainbow_bridge' as app,
    'Bridge' as category,
    bridge_volume.bridge_volume,
    bridge_dau.bridge_dau
from bridge_volume
left join bridge_dau on bridge_volume.date = bridge_dau.date
where bridge_volume.date < to_date(sysdate())
