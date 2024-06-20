{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_tvl("optimism", "0x4200000000000000000000000000000000000042") }}
