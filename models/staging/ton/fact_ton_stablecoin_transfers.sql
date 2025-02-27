{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{agg_chain_stablecoin_transfers("ton", contract_address)}}