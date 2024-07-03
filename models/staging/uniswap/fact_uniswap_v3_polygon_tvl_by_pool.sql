{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}
{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0x1F98431c8aD98523631AE4a59f267346ea31F984", "polygon", "uniswap"
    )
}}
