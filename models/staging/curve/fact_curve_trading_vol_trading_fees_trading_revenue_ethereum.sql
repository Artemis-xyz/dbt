{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_trading_vol_trading_fees_trading_revenue("ethereum") }}
