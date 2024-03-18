{{ config(materialized="table") }}
select date, bridge_volume, fees, inflow, outflow, app, chain, category
from {{ ref("fact_avalanche_bridge_bridge_volume") }}
