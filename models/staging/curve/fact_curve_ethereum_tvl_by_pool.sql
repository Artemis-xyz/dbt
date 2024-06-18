{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl_by_pool("ethereum", "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") }}
