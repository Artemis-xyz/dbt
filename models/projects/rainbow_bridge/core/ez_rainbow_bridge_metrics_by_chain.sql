{{
    config(
        materialized="table",
        snowflake_warehouse="RAINBOW_BRIDGE",
        database="rainbow_bridge",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume as (
        select date, chain, inflow, outflow
        from {{ ref("fact_rainbow_bridge_bridge_volume") }}
        where chain is not null
    )
select
    date,
    'rainbow_bridge' as app,
    'Bridge' as category,
    chain,
    inflow,
    outflow
from bridge_volume
where date < to_date(sysdate())
