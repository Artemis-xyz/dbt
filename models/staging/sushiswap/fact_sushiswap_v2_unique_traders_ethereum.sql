{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="SUSHISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
        "ethereum",
        "PairCreated",
        "pair",
        "sushiswap_v2",
    )
}}
