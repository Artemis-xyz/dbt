{{ config(snowflake_warehouse="OPTIMISM_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('optimism') }}