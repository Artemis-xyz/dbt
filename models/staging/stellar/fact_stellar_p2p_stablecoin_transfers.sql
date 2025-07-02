--depends_on: {{ ref("fact_stellar_stablecoin_transfers") }}
{{
    config(
        materialized="incremental",
        unique_key="unique_id",
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{ p2p_stablecoin_transfers("stellar", contract_address) }}