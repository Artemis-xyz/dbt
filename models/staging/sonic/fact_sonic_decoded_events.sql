{{ config(snowflake_warehouse="SONIC", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('sonic') }}
