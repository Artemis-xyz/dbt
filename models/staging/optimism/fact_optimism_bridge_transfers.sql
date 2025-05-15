{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('optimism', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'flipside') }}
