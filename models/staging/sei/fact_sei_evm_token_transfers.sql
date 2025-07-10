{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}
{{ token_transfer_events('sei') }}
qualify row_number() over ( partition by transaction_hash, event_index order by block_timestamp desc ) = 1