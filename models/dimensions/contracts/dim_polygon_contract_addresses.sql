{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="POLYGON"
    )
}}

{{distinct_contract_addresses("polygon")}}