{{ config(materialized="table", snowflake_warehouse="PANCAKESWAP_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0x02a84c1b3BBD7401a5f7fa98a384EBC70bB5749E", "base", "pancakeswap"
    )
}}
