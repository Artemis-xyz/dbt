{{
    config(
        materialized="incremental", unique_key=["tx_hash", "event_index"], snowflake_warehouse="CURVE"
    )
}}
{{ fact_curve_dex_swaps("optimism") }}
