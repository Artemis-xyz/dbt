
{{ config(
    materialized="table",
    warehouse="USDT0",
    database="USDT0",
    schema="core",
    alias="ez_metrics_by_chain"
) }}

with
    src_bridge_volume as (
        select date, source_chain as chain, sum(amount_usd) as outflow
        from {{ ref("fact_usdt0_flows") }}
        where source_chain is not null
        group by date, chain
    ),
    dst_bridge_volume as (
        select date, destination_chain as chain, sum(amount_usd) as inflow
        from {{ ref("fact_usdt0_flows") }}
        where destination_chain is not null
        group by date, chain
    )
select
    coalesce(src_bridge_volume.date, dst_bridge_volume.date) as date,
    'usdt0' as app,
    'Bridge' as category,
    coalesce(src_bridge_volume.chain, dst_bridge_volume.chain) as chain,
    coalesce(src_bridge_volume.outflow, 0) as outflow,
    coalesce(dst_bridge_volume.inflow, 0) as inflow
from src_bridge_volume
full join dst_bridge_volume using (date, chain)
where date < to_date(sysdate())
