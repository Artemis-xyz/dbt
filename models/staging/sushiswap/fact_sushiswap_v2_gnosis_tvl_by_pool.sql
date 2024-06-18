{{ config(materialized="table", snowflake_warehouse="SUSHISWAP_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0xc35DADB65012eC5796536bD9864eD8773aBc74C4", "gnosis", "sushiswap"
    )
}}
