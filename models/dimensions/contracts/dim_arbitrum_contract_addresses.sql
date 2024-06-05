{{ 
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM"
    )
}}

{{distinct_contract_addresses("arbitrum")}}