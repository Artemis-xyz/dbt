{{
    config(
        materialized="view",
        snowflake_warehouse="SUI",
        database="sui",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_sui_address_balances_by_token") }}