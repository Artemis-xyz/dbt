--depends_on: {{ ref("fact_stellar_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index", "token_address", "from_address", "to_address", "amount"],
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{ p2p_stablecoin_transfers("stellar", contract_address) }}