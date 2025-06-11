{{ config(snowflake_warehouse="BSC", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('bsc') }}