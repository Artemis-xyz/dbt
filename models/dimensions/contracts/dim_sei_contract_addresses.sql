{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="SEI",
        unique_key="contract_address"
    )
}}

{{distinct_contract_addresses("sei")}}
