{{ config(snowflake_warehouse="CODEX", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('codex') }}
