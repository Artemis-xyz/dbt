{{ config(snowflake_warehouse="AVALANCHE_MD", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ clean_flipside_evm_events('avalanche') }}
