{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32",
        "polygon",
        (),
        "quickswap",
        3000,
    )
}}
