{{ config(materialized="incremental", snowflake_warehouse="ABSTRACT", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events('abstract') }}
