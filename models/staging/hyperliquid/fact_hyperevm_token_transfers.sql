{{ config(snowflake_warehouse="HYPERLIQUID", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('hyperevm') }}
