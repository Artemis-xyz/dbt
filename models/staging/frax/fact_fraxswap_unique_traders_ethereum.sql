{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x43eC799eAdd63848443E2347C49f5f52e8Fe0F6f",
        "ethereum",
        "PairCreated",
        "pair",
        "fraxswap",
    )
}}
