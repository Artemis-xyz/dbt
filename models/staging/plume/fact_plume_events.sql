{{ config(materialized="incremental", snowflake_warehouse="PLUME", unique_key=["transaction_hash", "event_index"]) }}


{{ clean_goldsky_events_v2('plume_mainnet') }}
