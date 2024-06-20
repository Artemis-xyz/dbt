--depends_on {{ ref("fact_curve_avalanche_dex_swaps") }}

{{
    config(
        materialized="table", snowflake_warehouse="CURVE"
    )
}}
{{ fact_daily_curve_trading_vol_fees_traders_by_pool("avalanche", "fact_curve_avalanche_dex_swaps") }}
