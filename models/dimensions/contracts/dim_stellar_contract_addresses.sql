{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="STELLAR",
        unique_key="contract_address"
    )
}}

{{distinct_contract_addresses("stellar")}}
