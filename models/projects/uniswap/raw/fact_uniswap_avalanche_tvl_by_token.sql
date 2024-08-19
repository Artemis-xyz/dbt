{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_uniswap_avalanche_tvl_by_token",
    )
}}

{{
    fact_daily_uniswap_tvl_by_token(
        "avalanche"
    )
}}