{{
    config(
        materialized="view",
        database="ton",
        schema="core",
        alias="ez_address_balances_by_token",
        snowflake_warehouse="TON_MD",
    )
}}

select *
from {{ ref("fact_ton_address_balances_by_token") }}