{{
    config(
        materialized="view",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_avalanche_address_balances_by_token") }}