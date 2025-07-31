{{
    config(
        materialized="table",
        snowflake_warehouse="STARKNET",
        database="starknet",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume_metrics as (
        select date, chain, inflow, outflow
        from {{ ref("fact_starknet_bridge_bridge_volume") }}
        where chain is not null
    )
select
    bridge_volume_metrics.date
    , 'starknet' as artemis_id
    , bridge_volume_metrics.chain

    -- Standardized Metrics

    -- Usage Data
    , bridge_volume_metrics.inflow
    , bridge_volume_metrics.outflow

from bridge_volume_metrics
where bridge_volume_metrics.date < to_date(sysdate())
