{{ config(materialized="incremental", snowflake_warehouse="UNICHAIN", unique_key=["transaction_hash", "event_index"]) }}


{{ clean_goldsky_events_v2('unichain') }}
