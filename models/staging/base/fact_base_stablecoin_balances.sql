{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="STABLECOIN_V2_LG_2",
    )
}}

{% set contract_address = var('contract_address', "") %} 

{{ stablecoin_balances("base", contract_address) }}