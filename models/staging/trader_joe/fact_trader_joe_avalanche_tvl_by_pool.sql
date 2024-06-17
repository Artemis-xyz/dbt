{{ config(materialized="table", snowflake_warehouse="TRADER_JOE") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10", "avalanche", "trader_joe", version="v1"
    )
}}
