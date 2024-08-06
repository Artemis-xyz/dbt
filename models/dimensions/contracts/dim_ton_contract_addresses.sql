{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="TON",
        unique_key=["contract_address"]
    )
}}

{{distinct_contract_addresses("ton")}}
