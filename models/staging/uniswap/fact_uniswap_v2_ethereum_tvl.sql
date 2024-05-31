{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}

{{
    fact_daily_uniswap_fork_tvl(
        "ethereum", "uniswap", "v2", "fact_uniswap_v2_ethereum_tvl_by_pool"
    )
}}
