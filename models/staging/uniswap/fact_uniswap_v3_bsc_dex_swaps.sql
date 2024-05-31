{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_uniswap_v3_fork_dex_swaps(
        "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7", "bsc", (), "uniswap", "v3"
    )
}}
