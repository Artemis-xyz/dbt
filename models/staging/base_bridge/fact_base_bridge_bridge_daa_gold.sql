{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_base_bridge_bridge_daa") }}
