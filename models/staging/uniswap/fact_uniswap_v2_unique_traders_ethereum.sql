{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "ethereum",
        "PairCreated",
        "pair",
        "uniswap_v2",
    )
}}
