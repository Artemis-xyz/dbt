{{ config(materialized="incremental", snowflake_warehouse="CELO", unique_key=["transaction_hash", "event_index"]) }}

{{ token_transfer_events("celo") }}