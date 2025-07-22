{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="KATANA"
    )
}}

{{distinct_contract_addresses("katana")}}