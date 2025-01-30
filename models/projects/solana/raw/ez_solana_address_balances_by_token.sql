{{
    config(
        materialized="view",
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_solana_address_balances_by_token") }}