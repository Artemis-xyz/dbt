{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl_by_pool("optimism", "0x4200000000000000000000000000000000000042") }}
