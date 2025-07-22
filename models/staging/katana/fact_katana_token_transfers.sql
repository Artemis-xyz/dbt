{{ config(snowflake_warehouse="KATANA", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('katana') }}
