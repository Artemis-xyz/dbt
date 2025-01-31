{{
    config(
        materialized="view",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_optimism_address_balances_by_token") }}