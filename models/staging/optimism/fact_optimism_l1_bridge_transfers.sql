{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_superchain_l1_native_bridges('0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1', 'optimism') }}
