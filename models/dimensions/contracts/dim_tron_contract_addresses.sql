{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="TRON"
    )
}}

{{distinct_contract_addresses("tron")}}