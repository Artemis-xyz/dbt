{{ config(materialized="incremental", snowflake_warehouse="WORLDCHAIN", unique_key=["transaction_hash", "event_index"]) }}


{{ clean_goldsky_events('worldchain') }}
