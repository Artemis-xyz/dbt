{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_SM",
    )
}}

{{ p2p_transfer_volume("polygon") }}