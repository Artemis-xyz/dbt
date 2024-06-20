{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_tvl_by_pool("polygon", "0x0000000000000000000000000000000000001010") }}
