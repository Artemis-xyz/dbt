--depends_on: {{ ref("fact_solana_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
    )
}}

{{ p2p_stablecoin_transfers("solana") }}