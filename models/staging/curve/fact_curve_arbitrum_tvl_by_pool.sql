{{
    config(
        materialized="table", unique_key="date", snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_tvl_by_pool("arbitrum", "0x912ce59144191c1204e64559fe8253a0e49e6548") }}
