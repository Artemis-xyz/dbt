{{ config(materialized="table") }}
select date, bridge_volume, app, chain, category
from {{ ref("fact_stargate_bridge_volume") }}
