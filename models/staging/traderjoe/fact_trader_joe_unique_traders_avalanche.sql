{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
        "avalanche",
        "PairCreated",
        "pair",
        "trader_joe",
    )
}}
