{{ config(materialized="table", snowflake_warehouse="ETHEREUM_LG", unique_key=["transaction_hash", "event_index"]) }}

{{ decode_artemis_events("worldchain") }}
