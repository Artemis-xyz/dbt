{{ config(materialized="incremental", snowflake_warehouse="MANTLE", unique_key=["transaction_hash", "event_index"]) }}

{{ token_transfer_events("mantle") }}