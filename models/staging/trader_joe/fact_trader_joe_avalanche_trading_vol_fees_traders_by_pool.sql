--depends_on {{ ref("fact_trader_joe_avalanche_dex_swaps") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
    )
}}

{{
    fact_daily_uniswap_fork_trading_vol_fees_traders_by_pool(
        "avalanche",
        "trader_joe",
        "v1",
        "fact_trader_joe_avalanche_dex_swaps"
    )
}}
