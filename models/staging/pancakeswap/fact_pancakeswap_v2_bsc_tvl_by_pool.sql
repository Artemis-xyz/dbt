{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73", "bsc", "pancakeswap"
    )
}}
