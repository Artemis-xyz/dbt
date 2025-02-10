{{ config(materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_events("celo") }}
