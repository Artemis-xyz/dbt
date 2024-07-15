{{
    config(
        materialized="table",
        unique_key=["tx_hash", "index"],
    )
}}

{{ p2p_native_transfers("tron") }}