{{ 
    config(
        unique_key="contract_address",
        materialized="incremental",
        snowflake_warehouse="POLYGON"
    )
}}

{{distinct_contract_addresses("polygon")}}