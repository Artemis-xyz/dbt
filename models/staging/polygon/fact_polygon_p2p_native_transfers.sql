{{
    config(
        materialized="table",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="POLYGON_SM",
    )
}}

{{ p2p_native_transfers("polygon") }}