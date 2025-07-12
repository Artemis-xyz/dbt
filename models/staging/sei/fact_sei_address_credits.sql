{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index", "trace_index"],
        snowflake_warehouse="SEI",
    )
}}

{{ evm_address_credits("sei") }}