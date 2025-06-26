-- depends_on: {{ ref("fact_blast_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_MD",
        enabled=false
    )
}}

{{
    daily_address_balances(
        "blast",
        "ethereum",
        "0x4300000000000000000000000000000000000004",
        18,
    )
}}
