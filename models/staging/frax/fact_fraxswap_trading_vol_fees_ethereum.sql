{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0x43eC799eAdd63848443E2347C49f5f52e8Fe0F6f",
        "ethereum",
        (),
        "fraxswap",
        3000,
    )
}}
