{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_trading_vol_trading_fees_trading_revenue("polygon") }}
