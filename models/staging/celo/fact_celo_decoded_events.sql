{{ config(materialized="incremental", unique_key=["transaction_hash", "event_index"], snowflake_warehouse="CELO_LG") }}

{{ decode_artemis_events("celo") }}
