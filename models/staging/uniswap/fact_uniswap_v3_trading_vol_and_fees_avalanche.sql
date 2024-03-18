{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_v3_fork_trading_vol_and_fees(
        "0x740b1c1de25031C31FF4fC9A62f554A55cdC1baD", "avalanche", (), "uniswap_v3"
    )
}}
