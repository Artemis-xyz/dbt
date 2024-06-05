{{ 
    config(
        materialized="table",
        snowflake_warehouse="BASE"
    )
}}

{{distinct_contract_addresses("base")}}