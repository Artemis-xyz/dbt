{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "index"],
        snowflake_warehouse="BASE"
    )
}}

{{ clean_flipside_evm_transactions('base') }}