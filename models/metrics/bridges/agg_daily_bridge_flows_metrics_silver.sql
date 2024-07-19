{{ config(materialized="table") }}

with
    daily_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_across_flows"),
                    ref("fact_synapse_flows"),
                    ref("fact_wormhole_flows"),
                    ref("fact_arbitrum_one_bridge_flows"),
                    ref("fact_avalanche_bridge_flows"),
                    ref("fact_base_bridge_flows"),
                    ref("fact_optimism_bridge_flows"),
                    ref("fact_injective_bridge_flows"),
                    ref("fact_polygon_pos_bridge_flows"),
                    ref("fact_starknet_bridge_flows"),
                    ref("fact_zksync_era_bridge_flows"),
                    ref("fact_rainbow_bridge_flows"),
                ]
            )
        }}
    )
select
    *,
    date::string
    || '-'
    || app
    || '-'
    || source_chain
    || '-'
    || destination_chain
    || '-'
    || category as unique_id
from daily_data
where date < to_date(sysdate())
