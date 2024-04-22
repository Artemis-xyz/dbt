{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ p2p_native_transfers("ethereum") }}