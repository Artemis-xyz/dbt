{{ config(materialized="incremental", snowflake_warehouse="LINEA", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_goldsky_events('linea') }}
