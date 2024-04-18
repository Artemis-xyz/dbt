{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ p2p_transfer_volume("optimism") }}