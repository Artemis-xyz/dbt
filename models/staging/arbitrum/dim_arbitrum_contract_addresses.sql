{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="ARBITRUM"
    )
}}

{{distinct_contract_addresses("arbitrum")}}