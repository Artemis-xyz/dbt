{{ config(
    materialized="table",
    warehouse="DEBRIDGE",
    database="DEBRIDGE",
    schema="core",
    alias="ez_metrics_by_chain"
) }}
with
    bridge_volume as (
        select date, chain, inflow, outflow, fees
        from {{ ref("fact_debridge_fundamental_metrics_by_chain") }}
        where chain is not null
    )
select
    date
    , 'debridge' as artemis_id
    , chain

    -- Standardized Metrics

    -- Usage Data
    , bridge_volume.inflow
    , bridge_volume.outflow
    , bridge_volume.fees

from bridge_volume
where date < to_date(sysdate())
