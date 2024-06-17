{{ config(materialized="table", snowflake_warehouse="TRADER_JOE") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0xaE4EC9901c3076D0DdBe76A520F9E90a6227aCB7", "arbitrum", "trader_joe", version="v1"
    )
}}
