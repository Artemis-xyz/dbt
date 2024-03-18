{{
    config(
        materialized="incremental", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl("arbitrum", "0x912ce59144191c1204e64559fe8253a0e49e6548") }}
