{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl("ethereum", "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") }}
