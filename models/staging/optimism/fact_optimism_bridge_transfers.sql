{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('optimism', '0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1', 'flipside') }}
