{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl("avalanche", "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7") }}
