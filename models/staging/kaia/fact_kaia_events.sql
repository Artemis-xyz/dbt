{{ config(materialized="incremental", snowflake_warehouse="KAIA_LG", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_dune_evm_events("kaia") }}