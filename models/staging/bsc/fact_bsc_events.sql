{{ config(snowflake_warehouse="BSC", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_flipside_evm_events('bsc') }}
