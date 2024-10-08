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
    select date, sum(bridge_volume) as bridge_volume, sum(fees) as fees
    from {{ ref("fact_wormhole_bridge_volume_gold") }}
    group by 1
)

select
    coalesce(txns.date, daa.date) as date,
    coalesce(txns.txns, 0) as bridge_txns,
    coalesce(daa.bridge_daa, 0) as bridge_daa,
    coalesce(bridge_volume.fees, 0) as fees,
    coalesce(bridge_volume.bridge_volume, 0) as bridge_volume
from txns
left join daa on txns.date = daa.date
left join bridge_volume on txns.date = bridge_volume.date
where coalesce(txns.date, daa.date) < to_date(sysdate())
