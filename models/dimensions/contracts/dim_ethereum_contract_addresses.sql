{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="ETHEREUM"
    )
}}

{{distinct_contract_addresses("ethereum")}}