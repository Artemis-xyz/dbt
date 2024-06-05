{{ 
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM"
    )
}}

{{distinct_contract_addresses("optimism")}}