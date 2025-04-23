{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_flipside_evm_events('ethereum') }}
