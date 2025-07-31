{{ config(
    materialized="table",
    warehouse="WORMHOLE",
    database="WORMHOLE",
    schema="core",
    alias="ez_metrics_by_chain"
) }}

with
    bridge_volume as (
        select date, chain, inflow, outflow
        from {{ ref("fact_wormhole_bridge_volume_gold") }}
        where chain is not null
    )
select
    bridge_volume.date
    , 'wormhole' as artemis_id
    , bridge_volume.chain

    -- Standardized Metrics

    -- Usage Data
    , bridge_volume.inflow
    , bridge_volume.outflow

from bridge_volume
where date < to_date(sysdate())
