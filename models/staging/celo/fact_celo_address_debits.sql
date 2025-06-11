{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="CELO_MD",
    )
}}

{{ evm_address_debits("celo") }}