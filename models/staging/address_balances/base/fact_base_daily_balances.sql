-- depends_on: {{ ref("fact_base_address_balances_by_token") }}
{{ config(materialized="incremental", unique_key=["date", "address"]) }}

{{
    daily_address_balances(
        "base",
        "ethereum",
        "0x4200000000000000000000000000000000000006",
        18,
    )
}}
