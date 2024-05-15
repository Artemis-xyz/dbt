{{
    config(
        materialized="table",
        snowflake_warehouse="CCTP",
        database="cctp",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume as (
        select date, chain, inflow, outflow
        from {{ ref("fact_cctp_bridge_volume") }}
        where chain is not null
    )
select
    date,
    'cctp' as protocol,
    'bridge' as category,
    chain,
    inflow,
    outflow
from bridge_volume
where date < to_date(sysdate())
