{{ config(snowflake_warehouse="ETHEREUM_LG", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('ethereum') }}