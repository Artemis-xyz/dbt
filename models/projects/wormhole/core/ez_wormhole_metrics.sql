{{ config(
    materialized="table",
    warehouse="WORMHOLE",
    database="WORMHOLE",
    schema="core",
    alias="ez_metrics"
) }}

with txns as (
    select
        date,
        txns
    from {{ ref("fact_wormhole_txns") }}
)
, daa as (
    select
        date,
        bridge_daa
    from {{ ref("fact_wormhole_bridge_daa_gold") }}
)
, bridge_volume as (
    select date, bridge_volume
    from {{ ref("fact_wormhole_bridge_volume_gold") }}
)

select
    coalesce(txns.date, daa.date) as date,
    coalesce(txns.txns, 0) as bridge_txns,
    coalesce(daa.bridge_daa, 0) as bridge_daa,
    0 as fees,
    bridge_volume.bridge_volume
from txns
left join daa on txns.date = daa.date
left join bridge_volume on txns.date = bridge_volume.date
where coalesce(txns.date, daa.date) < to_date(sysdate())
