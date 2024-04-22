{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="TRON",
    )
}}

{{ p2p_token_transfers("tron") }}