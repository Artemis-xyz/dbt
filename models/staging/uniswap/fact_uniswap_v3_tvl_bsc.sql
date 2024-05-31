{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}

{{
    fact_daily_uniswap_v3_fork_tvl(
        "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7", "bsc", "uniswap_v3"
    )
}}