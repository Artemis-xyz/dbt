{{ config(materialized="table", snowflake_warehouse="UNISWAP_TVL_SM") }}
{{
    fact_daily_uniswap_v3_fork_tvl_by_pool(
        "0x792edAdE80af5fC680d96a2eD80A44247D2Cf6Fd", "blast", "uniswap"
    )
}}
