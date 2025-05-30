{{ config(materialized="incremental", snowflake_warehouse="PLUME", unique_key=["transaction_hash", "event_index"]) }}

{{ token_transfer_events("plume") }}