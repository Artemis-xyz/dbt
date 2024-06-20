{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_unique_traders("avalanche") }}
