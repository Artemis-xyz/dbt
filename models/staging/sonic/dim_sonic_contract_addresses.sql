{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="SONIC"
    )
}}

{{distinct_contract_addresses("sonic")}}