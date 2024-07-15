{{
    config(
        materialized="incremental", unique_key=["tx_hash", "event_index"]
    )
}}
{{ fact_curve_dex_swaps("polygon") }}
