{{ 
    config(
        materialized="table",
        snowflake_warehouse="POLYGON"
    )
}}

{{distinct_contract_addresses("polygon")}}