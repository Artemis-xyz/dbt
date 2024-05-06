{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
    )
}}

{{ p2p_transfer_volume("near") }}