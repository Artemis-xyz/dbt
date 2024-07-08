-- depends_on: {{ ref("ez_ton_address_balances_by_token") }}
{{
    config(
        materialized="table",
        database="ton",
        schema="core",
        name="ez_current_balances",
        snowflake_warehouse="TON_MD",
    )
}}

{{ current_balances("ton") }}