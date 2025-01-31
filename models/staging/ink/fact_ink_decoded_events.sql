{{ config(materialized="incremental", snowflake_warehouse="INK", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_goldsky_events("ink") }}
