--depends_on {{ ref("fact_curve_ethereum_dex_swaps") }}

{{
    config(
        materialized="table"
    )
}}
{{ fact_daily_curve_trading_vol_fees_traders_by_pool("ethereum", "fact_curve_ethereum_dex_swaps") }}
