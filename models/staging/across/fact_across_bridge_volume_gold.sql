{{ config(materialized="table") }}
select date, bridge_volume, fees, inflow, outflow, app, chain, category
from {{ ref("fact_across_bridge_volume") }}
