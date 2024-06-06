{{
    config(
        materialized="table",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="SOLANA",
    )
}}

{{ p2p_token_transfers("solana") }}