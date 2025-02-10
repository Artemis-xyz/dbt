{{ config(materialized="incremental", snowflake_warehouse='ANALYTICS_XL', unique_key=["transaction_hash", "event_index"]) }}

{{ decode_events("celo") }}
