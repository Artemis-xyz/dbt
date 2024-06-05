{{ 
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE"
    )
}}

{{distinct_contract_addresses("avalanche")}}