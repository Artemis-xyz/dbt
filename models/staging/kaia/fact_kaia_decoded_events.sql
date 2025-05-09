{{ config(snowflake_warehouse="KAIA", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('kaia') }}
