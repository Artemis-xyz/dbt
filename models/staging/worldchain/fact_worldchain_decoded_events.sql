{{ config(materialized="table", snowflake_warehouse="WORLDCHAIN", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_goldsky_events("worldchain") }}
