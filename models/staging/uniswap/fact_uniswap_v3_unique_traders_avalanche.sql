{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD",
        "avalanche",
        "PoolCreated",
        "pool",
        "uniswap_v3",
    )
}}
