{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume_metrics as (
        select date, chain, inflow, outflow
        from {{ ref("fact_optimism_bridge_bridge_volume") }}
        where chain is not null
    )
select
    date,
    'optimism' as app,
    'Bridge' as category,
    chain,
    inflow,
    outflow
from bridge_volume_metrics
where date < to_date(sysdate())
