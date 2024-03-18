-- depends_on: {{ ref("fact_bsc_address_balances_by_token") }}
{{
    config(
        materialized="incremental",
        unique_key=["date", "address"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{
    daily_address_balances(
        "bsc",
        "binancecoin",
        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        18,
    )
}}
