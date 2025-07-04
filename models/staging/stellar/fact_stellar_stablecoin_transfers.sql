-- depends_on: {{ ref('fact_stellar_stablecoin_contracts') }}
{{ config(
    materialized="incremental", 
        snowflake_warehouse="STABLECOIN_LG_2", 
        unique_key=["tx_hash", "index", "contract_address", "from_address", "to_address", "amount"],
    ) 
}}

{% set contract_address = var('contract_address', "") %} 

{{agg_chain_stablecoin_transfers("stellar", contract_address)}}