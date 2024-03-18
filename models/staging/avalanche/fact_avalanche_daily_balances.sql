-- depends_on: {{ ref("fact_avalanche_address_balances_by_token") }}
{{ config(materialized="incremental", unique_key=["date", "address"]) }}

{{
    daily_address_balances(
        "avalanche",
        "avalanche-2",
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        18,
    )
}}
