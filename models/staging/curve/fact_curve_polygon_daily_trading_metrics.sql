--depends_on {{ ref("fact_curve_polygon_dex_swaps") }}
{{
    config(
        materialized="table"
    )
}}
{{ fact_daily_curve_trading_vol_fees_traders_by_pool("polygon", "fact_curve_polygon_dex_swaps") }}
