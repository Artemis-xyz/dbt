{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="BSC"
    )
}}

{{distinct_contract_addresses("bsc")}}
