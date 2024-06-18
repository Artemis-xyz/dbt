{{ config(materialized="table", snowflake_warehouse="FRAX_SM") }}

{{
    fact_daily_uniswap_v2_fork_tvl_by_pool(
        "0x43eC799eAdd63848443E2347C49f5f52e8Fe0F6f", "ethereum", "fraxswap"
    )
}}
