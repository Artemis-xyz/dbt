{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="APTOS"
    )
}}

{{distinct_contract_addresses("aptos")}}