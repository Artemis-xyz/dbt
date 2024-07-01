{{ config(materialized="table") }}

{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7", "bsc", "uniswap"
    )
}}