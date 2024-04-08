{{ config(materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ transfer_events("celo") }}