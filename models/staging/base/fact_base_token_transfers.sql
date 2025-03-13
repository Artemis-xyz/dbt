{{ config(snowflake_warehouse="BASE", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('base') }}
