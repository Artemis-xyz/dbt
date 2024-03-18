{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl("optimism", "0x4200000000000000000000000000000000000042") }}
