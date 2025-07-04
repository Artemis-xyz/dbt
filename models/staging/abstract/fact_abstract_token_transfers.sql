{{ config(materialized="incremental", snowflake_warehouse="ABSTRACT", unique_key=["transaction_hash", "event_index"]) }}

{{ token_transfer_events("abstract") }}