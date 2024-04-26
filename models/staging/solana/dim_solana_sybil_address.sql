{{ config(materialized="table", snowflake_warehouse="SOLANA") }}

{{ detect_sybil("solana") }}
