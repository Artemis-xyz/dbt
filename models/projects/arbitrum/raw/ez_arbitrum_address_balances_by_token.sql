{{
    config(
        materialized="view",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_arbitrum_address_balances_by_token") }}