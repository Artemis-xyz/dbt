{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}

with 
combined_events as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_stargate_v2_arbitrum_event_OFTReceived"),
                ref("fact_stargate_v2_avalanche_event_OFTReceived"),
                ref("fact_stargate_v2_base_event_OFTReceived"),
                ref("fact_stargate_v2_bsc_event_OFTReceived"),
                ref("fact_stargate_v2_ethereum_event_OFTReceived"),
                ref("fact_stargate_v2_optimism_event_OFTReceived"),
                ref("fact_stargate_v2_polygon_event_OFTReceived"),
                ref("fact_stargate_v2_sei_event_OFTReceived"),
                ref("fact_stargate_v2_berachain_event_OFTReceived"),
                ref("fact_stargate_v2_mantle_event_OFTReceived"),
            ],
        )
    }}
)
select
    block_timestamp
    , tx_hash
    , event_index
    , event_name
    , dst_chain
    , chain_ids.chain as src_chain
    , dst_address
    , guid
    , token_messaging_address
    , stargate_implementation_pool
    , token_address
    , symbol
    , decimals
    , amount_received_native
    , amount_received_adjusted
    , amount_received
    , tx_status
from combined_events
left join {{ref("stargate_chain_ids")}} chain_ids on src_e_id = id
where guid <> '0x0000000000000000000000000000000000000000000000000000000000000000' and tx_status = 'SUCCESS'