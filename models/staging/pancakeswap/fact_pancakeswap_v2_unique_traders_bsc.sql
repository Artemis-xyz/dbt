{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}


{{
    fact_daily_uniswap_fork_unique_traders(
        "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
        "bsc",
        "PairCreated",
        "pair",
        "pancakeswap_v2",
    )
}}
