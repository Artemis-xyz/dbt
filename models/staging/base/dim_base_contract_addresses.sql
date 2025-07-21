{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="BASE"
    )
}}

{{distinct_contract_addresses("base")}}
