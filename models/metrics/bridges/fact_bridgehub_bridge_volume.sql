{{ config(materialized="table") }}

select *
from {{ ref("fact_across_bridge_volume_gold") }}
union
select *
from {{ ref("fact_arbitrum_one_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_avalanche_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_base_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_optimism_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_polygon_pos_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_starknet_bridge_bridge_volume_gold") }}
union
select *
from {{ ref("fact_synapse_bridge_volume_gold") }}
union
select *
from {{ ref("fact_wormhole_bridge_volume_gold") }}
union
select *
from {{ ref("fact_zksync_era_bridge_bridge_volume_gold") }}
