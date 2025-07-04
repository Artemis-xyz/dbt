-- depends_on: {{ ref('fact_ethereum_decoded_events') }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="PENDLE",
        unique_key = ["tx_hash", "event_index"]
    )
}}

{{ get_pendle_limit_order_events_for_chain('ethereum') }}