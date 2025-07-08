{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="BLAST"
    )
}}

{{distinct_contract_addresses("blast")}}
