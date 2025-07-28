{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="AVALANCHE"
    )
}}

{{distinct_contract_addresses("avalanche")}}