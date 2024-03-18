{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
    )
}}

{{ address_debits("base", "0x4200000000000000000000000000000000000006") }}
