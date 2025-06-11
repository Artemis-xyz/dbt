{{ config(materialized="incremental", snowflake_warehouse="BOB", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events("bob") }}
