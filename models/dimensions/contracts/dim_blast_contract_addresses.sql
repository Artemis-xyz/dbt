{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="BLAST"
    )
}}

{{distinct_contract_addresses("blast")}}
