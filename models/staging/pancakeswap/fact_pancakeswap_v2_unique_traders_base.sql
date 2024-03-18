{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}


{{
    fact_daily_uniswap_fork_unique_traders(
        "0x02a84c1b3BBD7401a5f7fa98a384EBC70bB5749E",
        "base",
        "PairCreated",
        "pair",
        "pancakeswap_v2",
    )
}}
