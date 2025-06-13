{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="SONIC"
    )
}}

{{distinct_contract_addresses("sonic")}}