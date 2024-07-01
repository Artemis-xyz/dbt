{{ config(materialized="table") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", "ethereum", "uniswap"
    )
}}
