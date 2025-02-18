{{ config(materialized="incremental", snowflake_warehouse="BERACHAIN", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_goldsky_events("berachain") }}
