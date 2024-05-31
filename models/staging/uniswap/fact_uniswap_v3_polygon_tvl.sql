{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}
{{
    fact_daily_uniswap_fork_tvl(
        "polygon", "uniswap", "v3", "fact_uniswap_v3_polygon_tvl_by_pool"
    )
}}
