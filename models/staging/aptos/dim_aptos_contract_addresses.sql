{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="APTOS_LG"
    )
}}

{{distinct_contract_addresses("aptos")}}