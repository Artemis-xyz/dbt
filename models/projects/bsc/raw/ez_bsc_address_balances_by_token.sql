{{
    config(
        materialized="view",
        snowflake_warehouse="BSC",
        database="bsc",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_bsc_address_balances_by_token") }}