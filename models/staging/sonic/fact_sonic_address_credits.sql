{{ config(snowflake_warehouse="BALANCES_LG", materialized="incremental", unique_key=["transaction_hash", "event_index", "trace_index"])}}

{{ evm_address_credits("sonic") }}