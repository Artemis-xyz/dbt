{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DEX_SM",
    )
}}

{{
    fact_daily_uniswap_fork_unique_traders(
        "0xaE4EC9901c3076D0DdBe76A520F9E90a6227aCB7",
        "arbitrum",
        "PairCreated",
        "pair",
        "trader_joe",
    )
}}
