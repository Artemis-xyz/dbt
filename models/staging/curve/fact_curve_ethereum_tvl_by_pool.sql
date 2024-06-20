{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_tvl_by_pool("ethereum", "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") }}
