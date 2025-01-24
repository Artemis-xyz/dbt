{{ config(materialized="table", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_goldsky_events("soneium") }}
