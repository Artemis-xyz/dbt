-- depends_on: {{ ref("fact_ethereum_address_balances_by_token") }}
{{
    config(
        materialized="table",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{
    daily_address_balances(
        "ethereum",
        "ethereum",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        18,
    )
}}
