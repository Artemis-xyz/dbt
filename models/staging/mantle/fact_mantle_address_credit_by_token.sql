{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="MANTLE",
    )
}}

{{ address_credits_dune("mantle") }}