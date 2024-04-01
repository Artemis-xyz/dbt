{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PANCAKESWAP_SM",
    )
}}

{{
    fact_daily_uniswap_v3_fork_trading_vol_and_fees(
        "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
        "base",
        (),
        "pancakeswap_v3",
    )
}}
