{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0x1097053Fd2ea711dad45caCcc45EfF7548fCB362",
        "ethereum",
        (),
        "pancakeswap_v2",
        2500,
    )
}}
