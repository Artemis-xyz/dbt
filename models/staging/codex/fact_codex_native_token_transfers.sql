{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "trace_index"],
        snowflake_warehouse="CODEX"
    )
}}

