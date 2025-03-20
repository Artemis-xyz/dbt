{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "trace_address"],
        snowflake_warehouse="BASE"
    )
}}

{{ clean_flipside_evm_transactions('base') }}