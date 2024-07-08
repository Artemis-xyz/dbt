{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_tvl_by_pool("avalanche", "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7") }}
