{{ config(materialized="table", snowflake_warehouse='USDT0') }}

{{
    dbt_utils.union_relations(
        relations=[
            ref("fact_usdt0_arbitrum_event_OFTReceived"),
            ref("fact_usdt0_berachain_event_OFTReceived"),
            ref("fact_usdt0_ethereum_event_OFTReceived"),
            ref("fact_usdt0_hyperevm_event_OFTReceived"),
            ref("fact_usdt0_optimism_event_OFTReceived"),
            ref("fact_usdt0_unichain_event_OFTReceived"),
            ref("fact_usdt0_ink_event_OFTReceived"),
        ],
    )
}}