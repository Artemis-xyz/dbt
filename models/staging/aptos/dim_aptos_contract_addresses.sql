{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="APTOS_LG"
    )
}}

{{distinct_contract_addresses("aptos")}}