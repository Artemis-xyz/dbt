{{
    config(
        materialized="view",
        database="ton",
        schema="core",
        name="ez_current_balances",
        snowflake_warehouse="TON_MD",
    )
}}

select *
from {{ ref("dim_ton_current_balances") }}