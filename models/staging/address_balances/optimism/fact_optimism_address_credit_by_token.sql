{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
    )
}}

{{ address_credits("optimism") }}
