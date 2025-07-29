{{ config(
    materialized="incremental", 
        snowflake_warehouse="KAIA", 
        unique_key=["tx_hash", "index"],
    ) 
}}

{% set contract_address = var('contract_address', "") %} 

{{agg_chain_stablecoin_transfers("katana", contract_address)}}
