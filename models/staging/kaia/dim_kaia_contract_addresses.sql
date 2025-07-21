{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="KAIA"
    )
}}

{{distinct_contract_addresses("kaia")}}