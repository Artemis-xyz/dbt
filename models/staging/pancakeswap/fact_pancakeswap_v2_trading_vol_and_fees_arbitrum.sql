{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0x02a84c1b3BBD7401a5f7fa98a384EBC70bB5749E",
        "arbitrum",
        (),
        "pancakeswap_v2",
        2500,
    )
}}
