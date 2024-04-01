-- depends_on: {{ ref("fact_tron_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

-- chain
-- coingecko_id
-- Wrapped Native Token
-- Wrapped Native Token Decimals
{{
    daily_address_balances(
        "tron",
        "tron",
        "TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR",
        6,
    )
}}
