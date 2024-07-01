{{ config(materialized="table") }}


{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD", "avalanche", "uniswap"
    )
}}
