{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_credits("bsc", "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c") }}
