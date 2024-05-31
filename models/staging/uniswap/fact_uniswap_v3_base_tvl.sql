{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}


{{
    fact_daily_uniswap_fork_tvl(
        "base", "uniswap", "v3", "fact_uniswap_v3_base_tvl_by_pool"
    )
}}
