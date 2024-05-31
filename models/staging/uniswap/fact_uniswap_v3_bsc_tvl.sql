{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}

{{
    fact_daily_uniswap_fork_tvl(
        "bsc", "uniswap", "v3", "fact_uniswap_v3_bsc_tvl_by_pool"
    )
}}