{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="MANTLE"
    )
}}

{{distinct_contract_addresses("mantle")}}