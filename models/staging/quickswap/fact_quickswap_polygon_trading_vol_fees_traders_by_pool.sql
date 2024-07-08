--depends_on {{ ref("fact_quickswap_polygon_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="QUICKSWAP",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "polygon",
        "quickswap",
        "v2",
        "fact_quickswap_polygon_dex_swaps"
    )
}}
