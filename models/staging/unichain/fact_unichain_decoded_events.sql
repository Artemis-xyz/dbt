{{ config(materialized="table", snowflake_warehouse="ETHEREUM", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events("unichain") }}
