{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_avalanche_bridge_bridge_daa") }}
