-- depends_on: {{ ref("fact_ton_address_credit_by_token") }}
-- depends_on: {{ ref("fact_ton_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "ez_ton_current_balances") }}
{{
    config(
        materialized="table",
        database="ton",
        schema="core",
        name="ez_address_balances_by_token",
        snowflake_warehouse="TON_MD",
    )
}}

{{ address_balances("ton") }}