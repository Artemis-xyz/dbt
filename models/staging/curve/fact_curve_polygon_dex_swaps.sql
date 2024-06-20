{{
    config(
        materialized="incremental", unique_key=["tx_hash", "event_index"], snowflake_warehouse="CURVE_SM"
    )
}}
{{ fact_curve_dex_swaps("polygon") }}
