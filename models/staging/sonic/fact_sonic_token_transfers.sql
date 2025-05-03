{{ config(snowflake_warehouse="SONIC", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('sonic') }}
