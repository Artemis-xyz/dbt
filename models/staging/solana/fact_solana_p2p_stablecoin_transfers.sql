{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="SOLANA",
    )
}}

{{ p2p_stablecoin_transfers("solana") }}