{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume_metrics as (
        select date, chain, inflow, outflow
        from {{ ref("fact_avalanche_bridge_bridge_volume") }}
        where chain is not null
    )
select
    date,
    'avalanche' as artemis_id,
    chain,

    --Bridge Data
    inflow,
    outflow
from bridge_volume_metrics
where date < to_date(sysdate())
