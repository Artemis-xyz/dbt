--depends_on {{ ref("fact_curve_optimism_dex_swaps") }}

{{
    config(
        materialized="table", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_daily_curve_trading_vol_fees_traders_by_pool("optimism", "fact_curve_optimism_dex_swaps") }}
