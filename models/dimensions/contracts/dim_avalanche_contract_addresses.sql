{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="AVALANCHE"
    )
}}

{{distinct_contract_addresses("avalanche")}}