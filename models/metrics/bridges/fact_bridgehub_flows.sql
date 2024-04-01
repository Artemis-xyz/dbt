{{ config(materialized="table") }}

select *
from {{ ref("fact_across_flows") }}
union
select *
from {{ ref("fact_synapse_flows") }}
union
select *
from {{ ref("fact_wormhole_flows") }}
union
select *
from {{ ref("fact_arbitrum_one_bridge_flows") }}
union
select *
from {{ ref("fact_avalanche_bridge_flows") }}
union
select *
from {{ ref("fact_base_bridge_flows") }}
union
select *
from {{ ref("fact_optimism_bridge_flows") }}
union
select *
from {{ ref("fact_polygon_pos_bridge_flows") }}
union
select *
from {{ ref("fact_starknet_bridge_flows") }}
union
select *
from {{ ref("fact_zksync_era_bridge_flows") }}
