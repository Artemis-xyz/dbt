--depends_on {{ ref("fact_curve_arbitrum_dex_swaps") }}

{{
    config(
        materialized="table", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_daily_curve_trading_vol_fees_traders_by_pool("arbitrum", "fact_curve_arbitrum_dex_swaps") }}
