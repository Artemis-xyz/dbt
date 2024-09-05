{{ config(
    materialized="table",
    warehouse="WORMHOLE",
    database="WORMHOLE",
    schema="CORE",
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

select
    coalesce(txns.date, daa.date) as date,
    coalesce(txns.txns, 0) as txns,
    coalesce(daa.bridge_daa, 0) as bridge_daa,
    0 as fees
from txns
left join daa on txns.date = daa.date
where coalesce(txns.date, daa.date) < to_date(sysdate())
