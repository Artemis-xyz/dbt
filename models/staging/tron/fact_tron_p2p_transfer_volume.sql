{{
    config(
        materialized="table",
        snowflake_warehouse="TRON",
    )
}}

{{ p2p_transfer_volume("tron") }}