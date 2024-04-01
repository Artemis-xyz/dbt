{{ config(materialized="table") }}

select date, bridge_daa, app, chain, category
from {{ ref("fact_synapse_bridge_daa") }}
