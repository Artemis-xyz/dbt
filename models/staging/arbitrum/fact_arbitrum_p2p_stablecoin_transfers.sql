--depends_on: {{ ref("fact_arbitrum_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{ p2p_stablecoin_transfers("arbitrum", contract_address) }}