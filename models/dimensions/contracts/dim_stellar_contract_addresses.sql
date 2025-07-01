{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="STELLAR"
    )
}}

{{distinct_contract_addresses("stellar")}}
