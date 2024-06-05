{{
    config(
        materialized="table",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="TRON",
    )
}}

{{ p2p_stablecoin_transfers("tron") }}