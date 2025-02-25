{{ config(materialized="table", snowflake_warehouse="ETHEREUM", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_goldsky_events("unichain") }}
