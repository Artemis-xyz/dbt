{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('ink', '0x88FF1e5b602916615391F55854588EFcBB7663f0', 'artemis') }}
