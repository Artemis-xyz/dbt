{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_across_bridge_daa") }}
