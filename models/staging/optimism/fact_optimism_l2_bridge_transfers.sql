{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_superchain_l2_native_bridges_flipside('optimism') }}
