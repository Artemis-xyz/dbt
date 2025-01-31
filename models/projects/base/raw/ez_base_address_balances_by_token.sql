{{
    config(
        materialized="view",
        snowflake_warehouse="BASE",
        database="base",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_base_address_balances_by_token") }}