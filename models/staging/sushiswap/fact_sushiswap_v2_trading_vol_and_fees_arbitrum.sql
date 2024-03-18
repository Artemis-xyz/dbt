{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="SUSHISWAP_SM",
    )
}}

{{
    fact_daily_uniswap_v2_fork_trading_vol_and_fees(
        "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
        "arbitrum",
        (),
        "sushiswap_v2",
        3000,
    )
}}
