{{ 
    config(
        materialized="incremental",
        unique_key="contract_address",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="dim_contract_addresses",
    )
}}

{{distinct_contract_addresses("ethereum")}}