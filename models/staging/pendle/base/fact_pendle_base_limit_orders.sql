-- depends_on: {{ ref('fact_base_decoded_events') }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        unique_key = ["tx_hash", "event_index"]
    )
}}

{{ get_pendle_limit_order_events_for_chain('base') }}