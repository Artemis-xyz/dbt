{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
    )
}}

{{ p2p_transfer_volume("base") }}