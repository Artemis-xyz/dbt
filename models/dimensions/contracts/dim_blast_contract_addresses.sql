{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="BSC"
    )
}}

{{distinct_contract_addresses("bsc")}}