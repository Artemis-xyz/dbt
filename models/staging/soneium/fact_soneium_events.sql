{{ config(materialized="incremental", snowflake_warehouse="SONEIUM", unique_key=["transaction_hash", "event_index"]) }}


{{ clean_goldsky_events('soneium') }}
