{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="MANTLE"
    )
}}

{{distinct_contract_addresses("mantle")}}