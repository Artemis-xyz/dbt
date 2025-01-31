{{
    config(
        materialized="view",
        snowflake_warehouse="TRON",
        database="tron",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_tron_address_balances_by_token") }}