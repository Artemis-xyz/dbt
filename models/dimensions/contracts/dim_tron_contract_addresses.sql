{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="TRON_LG"
    )
}}

{{distinct_contract_addresses("tron")}}