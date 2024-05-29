{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}


{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0x33128a8fC17869897dcE68Ed026d694621f6FDfD", "base", "uniswap_v3"
    )
}}
