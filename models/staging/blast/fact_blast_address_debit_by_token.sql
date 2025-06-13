{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_MD",
        enabled=false
    )
}}

{{ address_debits("blast", "0x4300000000000000000000000000000000000004") }}
