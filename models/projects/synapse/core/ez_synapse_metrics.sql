{{
    config(
        materialized="table",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_synapse_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_synapse_bridge_daa") }}
    )
select
    bridge_volume.date as date,
    'synapse' as app,
    'Bridge' as category,
    bridge_volume_metrics.bridge_volume,
    bridge_daa_metrics.bridge_daa
from bridge_volume_metrics
left join bridge_daa_metrics on bridge_volume_metrics.date = bridge_daa_metrics.date
where bridge_volume.date < to_date(sysdate())
