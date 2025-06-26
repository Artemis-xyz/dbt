{{ config(snowflake_warehouse="AVALANCHE_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('avalanche') }}
