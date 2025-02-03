{{
    config(
        materialized="view",
        snowflake_warehouse="MANTLE",
        database="mantle",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_mantle_address_balances_by_token") }}