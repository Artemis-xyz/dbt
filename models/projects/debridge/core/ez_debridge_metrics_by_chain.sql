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
    date,
    'debridge' as app,
    'Bridge' as category,
    chain,
    inflow,
    outflow,
    fees
from bridge_volume
where date < to_date(sysdate())
