{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ p2p_native_transfers("avalanche") }}