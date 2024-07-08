{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="BASE"
    )
}}

{{distinct_contract_addresses("base")}}
