{{
    config(
        materialized="table",
        snowflake_warehouse="CCTP",
        database="cctp",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    bridge_volume as (
        select date, bridge_volume
        from {{ ref("fact_cctp_bridge_volume") }}
        where chain is null
    ),
    bridge_dau as (
        select date, bridge_dau
        from {{ ref("fact_cctp_bridge_dau") }}
    )
select
    bridge_volume.date as date,
    'cctp' as protocol,
    'bridge' as category,
    bridge_volume.bridge_volume,
    bridge_dau.bridge_dau
from bridge_volume
left join bridge_dau on bridge_volume.date = bridge_dau.date
where bridge_volume.date < to_date(sysdate())
