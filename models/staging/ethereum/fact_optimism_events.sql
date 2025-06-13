{{ config(snowflake_warehouse="OPTIMISM_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_flipside_evm_events('optimism') }}