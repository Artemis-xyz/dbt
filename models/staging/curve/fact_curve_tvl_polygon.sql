{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_tvl("polygon", "0x0000000000000000000000000000000000001010") }}
