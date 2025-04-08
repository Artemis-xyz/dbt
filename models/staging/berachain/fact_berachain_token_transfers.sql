{{ config(materialized="incremental", snowflake_warehouse="BERACHAIN", unique_key=["transaction_hash", "event_index"]) }}

{{ token_transfer_events('berachain') }}
