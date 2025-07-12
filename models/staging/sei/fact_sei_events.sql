{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_flipside_evm_events('sei') }}
