{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="OPTIMISM"
    )
}}

{{distinct_contract_addresses("optimism")}}