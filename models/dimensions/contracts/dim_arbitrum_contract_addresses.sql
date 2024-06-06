{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="ARBITRUM"
    )
}}

{{distinct_contract_addresses("arbitrum")}}