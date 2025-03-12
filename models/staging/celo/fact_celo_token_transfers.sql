{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="CELO_MD"
    )
}}
{{ token_transfer_events("celo") }}