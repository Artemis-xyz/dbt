{{ config(materialized="incremental", snowflake_warehouse="PLUME", unique_key=["transaction_hash", "event_index"]) }}


{{ decode_artemis_events('plume') }}