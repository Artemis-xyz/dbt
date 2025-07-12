--depends_on: {{ ref("fact_sei_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{ p2p_stablecoin_transfers("sei", contract_address) }}