{{
    config(
        materialized="table",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume as (
        select date, chain, inflow, outflow
        from {{ ref("fact_across_bridge_volume") }}
        where chain is not null
    )
select
    date,
    'across' as artemis_id,
    chain,

    --Bridge Data
    inflow,
    outflow
from bridge_volume
where date < to_date(sysdate())
