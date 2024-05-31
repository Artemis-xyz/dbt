{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl(
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", "ethereum", "uniswap_v2"
    )
}}

