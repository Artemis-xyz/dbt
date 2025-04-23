{{ config(snowflake_warehouse="BASE", materialized="incremental", unique_key=["transaction_hash", "event_index", "trace_index"])}}

{{ evm_address_balances("base") }}