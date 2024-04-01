-- depends_on: {{ ref("fact_optimism_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

{{
    daily_address_balances(
        "optimism",
        "ethereum",
        "0x4200000000000000000000000000000000000006",
        18,
    )
}}
