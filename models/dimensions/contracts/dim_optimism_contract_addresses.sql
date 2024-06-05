{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM"
    )
}}

{{distinct_contract_addresses("optimism")}}