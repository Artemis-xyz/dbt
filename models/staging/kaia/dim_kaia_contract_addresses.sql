{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="KAIA"
    )
}}

{{distinct_contract_addresses("kaia")}}