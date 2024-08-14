{{ 
    config(
        materialized="incremental",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="dim_contract_addresses",
    )
}}

{{distinct_contract_addresses("ethereum")}}