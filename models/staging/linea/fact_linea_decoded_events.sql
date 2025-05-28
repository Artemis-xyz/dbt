{{ config(materialized="incremental", snowflake_warehouse="LINEA", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events("linea") }}