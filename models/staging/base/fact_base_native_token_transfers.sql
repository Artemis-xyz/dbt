{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "trace_index"],
        snowflake_warehouse="BASE_MD"
    )
}}

{{ clean_flipside_evm_native_token_transfers('base') }}