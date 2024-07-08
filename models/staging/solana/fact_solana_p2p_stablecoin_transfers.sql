--depends_on: {{ ref("fact_solana_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="SOLANA",
    )
}}

{{ p2p_stablecoin_transfers("solana") }}