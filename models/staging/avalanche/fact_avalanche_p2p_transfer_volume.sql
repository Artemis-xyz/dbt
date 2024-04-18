{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ p2p_transfer_volume("avalanche") }}