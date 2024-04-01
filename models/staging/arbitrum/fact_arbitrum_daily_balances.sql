-- depends_on: {{ ref("fact_arbitrum_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

{{
    daily_address_balances(
        "arbitrum",
        "ethereum",
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        18,
    )
}}
