{{ config(materialized="table", snowflake_warehouse="DEX_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl(
        "0xaE4EC9901c3076D0DdBe76A520F9E90a6227aCB7", "arbitrum", "trader_joe"
    )
}}
