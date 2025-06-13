{{ config(materialized="table", snowflake_warehouse="CODEX", unique_key=["transaction_hash"]) }}

{{ clean_goldsky_transactions("codex") }}