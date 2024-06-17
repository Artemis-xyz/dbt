{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
        "avalanche",
        (),
        "trader_joe",
        3000,
    )
}}
