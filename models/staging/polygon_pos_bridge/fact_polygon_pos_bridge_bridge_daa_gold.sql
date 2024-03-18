{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_polygon_pos_bridge_bridge_daa") }}
