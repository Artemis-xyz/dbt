{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_wormhole_bridge_daa") }}
