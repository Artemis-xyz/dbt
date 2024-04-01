{{ config(materialized="table") }}
select date, bridge_daa, app, chain, category
from {{ ref("fact_arbitrum_one_bridge_bridge_daa") }}
