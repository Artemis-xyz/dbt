{{ config(materialized="incremental", unique_key=["tx_hash", "event_index"]) }}

{{ token_transfer_events("mantle") }}