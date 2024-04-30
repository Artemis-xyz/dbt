{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA",
    )
}}

{{ p2p_transfer_volume("solana") }}