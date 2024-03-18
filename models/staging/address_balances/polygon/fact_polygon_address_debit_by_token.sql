{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_debits("polygon", "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270") }}
