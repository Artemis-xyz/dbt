{{ config(materialized="incremental", snowflake_warehouse="BOB", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_goldsky_events_v2('bob') }}
