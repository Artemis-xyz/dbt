--depends_on {{ ref("fact_trader_joe_arbitrum_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "arbitrum",
        "trader_joe",
        "v1",
        "fact_trader_joe_arbitrum_dex_swaps"
    )
}}
