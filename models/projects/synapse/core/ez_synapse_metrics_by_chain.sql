{{
    config(
        materialized="table",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume_metrics as (
        select date, chain, inflow, outflow
        from {{ ref("fact_synapse_bridge_volume") }}
        where chain is not null
    )
select
    bridge_volume_metrics.date
    , 'synapse' as artemis_id
    , bridge_volume_metrics.chain

    -- Standardized Metrics

    -- Usage Data
    , bridge_volume_metrics.inflow as inflow
    , bridge_volume_metrics.outflow as outflow
    
from bridge_volume_metrics
where date < to_date(sysdate())
