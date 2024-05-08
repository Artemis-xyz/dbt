{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="POLYGON",
    )
}}

{{ p2p_native_transfers("polygon") }}