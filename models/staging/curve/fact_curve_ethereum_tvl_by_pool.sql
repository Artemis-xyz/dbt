{{
    config(
        materialized="table", unique_key="date"
    )
}}
{{ fact_curve_tvl_by_pool("ethereum", "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE") }}
