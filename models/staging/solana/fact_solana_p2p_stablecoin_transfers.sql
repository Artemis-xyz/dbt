{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
    )
}}

{{ p2p_stablecoin_transfers("solana") }}