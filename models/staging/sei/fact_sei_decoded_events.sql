{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('sei') }}
