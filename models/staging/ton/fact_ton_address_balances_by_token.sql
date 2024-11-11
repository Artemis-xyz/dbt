-- depends_on: {{ ref("fact_ton_address_credit_by_token") }}
-- depends_on: {{ ref("fact_ton_address_debit_by_token") }}
-- depends_on: {{ source("BALANCES", "dim_ton_current_balances") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="TON_MD",
    )
}}

{{ address_balances("ton") }}