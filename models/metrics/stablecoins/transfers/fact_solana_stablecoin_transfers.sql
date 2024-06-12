-- depends_on: {{ ref('fact_solana_stablecoin_contracts') }}
{{ config(
    materialized="incremental", 
        snowflake_warehouse="BAM_TRANSACTION_2XLG", 
        unique_key=["tx_hash", "index"],
    ) 
}}
{{ agg_chain_stablecoin_transfers("solana") }}
