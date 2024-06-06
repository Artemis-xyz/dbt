-- depends_on: {{ ref('fact_solana_stablecoin_contracts') }}
{{ config(
    materialized="incremental", 
        snowflake_warehouse="STABLECOIN_LG_2", 
        unique_key=["tx_hash", "index"],
    ) 
}}
{{ agg_chain_stablecoin_transfers("solana") }}
