{{ config(materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_dune_evm_events("katana") }}