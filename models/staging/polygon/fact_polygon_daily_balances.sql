-- depends_on: {{ ref("fact_polygon_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{
    daily_address_balances(
        "polygon",
        "matic-network",
        "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        18,
    )
}}
