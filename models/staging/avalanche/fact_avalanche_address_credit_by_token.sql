{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
    )
}}

{{ address_credits("avalanche", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7") }}
