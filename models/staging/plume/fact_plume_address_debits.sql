{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

{{ evm_address_debits("plume") }}
