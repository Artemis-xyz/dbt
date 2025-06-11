{{ config(snowflake_warehouse="BASE_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('base') }}
