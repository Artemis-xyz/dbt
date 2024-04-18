{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_XS",
    )
}}

{{ p2p_transfer_volume("ethereum") }}