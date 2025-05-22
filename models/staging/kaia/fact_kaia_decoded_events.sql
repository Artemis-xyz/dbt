{{ config(snowflake_warehouse="KAIA_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('kaia') }}
