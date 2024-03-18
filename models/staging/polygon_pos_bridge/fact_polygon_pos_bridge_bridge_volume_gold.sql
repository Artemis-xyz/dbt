{{ config(materialized="table") }}
select date, bridge_volume, fees, inflow, outflow, app, chain, category
from {{ ref("fact_polygon_pos_bridge_bridge_volume") }}
