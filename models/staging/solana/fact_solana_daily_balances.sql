-- depends_on: {{ ref("fact_solana_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="STABLECOIN_LG",
    )
}}

{{
    daily_address_balances(
        "solana",
        "solana",
        "So11111111111111111111111111111111111111112",
        9,
    )
}}
