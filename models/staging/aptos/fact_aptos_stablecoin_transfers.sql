{{ config(
    materialized="incremental", 
        snowflake_warehouse="APTOS", 
        unique_key=["tx_hash", "index"],
    ) 
}}

{% set contract_address = var('contract_address', "") %} 

{{agg_chain_stablecoin_transfers("aptos", contract_address)}}
