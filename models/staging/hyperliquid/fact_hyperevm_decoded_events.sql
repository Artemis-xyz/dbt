{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events("hyperevm") }}
