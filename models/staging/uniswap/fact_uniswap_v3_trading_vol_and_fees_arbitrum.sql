{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="UNISWAP_SM",
    )
}}
{{
    fact_daily_uniswap_v3_fork_trading_vol_and_fees(
        "0x1F98431c8aD98523631AE4a59f267346ea31F984", "arbitrum", (), "uniswap_v3"
    )
}}
