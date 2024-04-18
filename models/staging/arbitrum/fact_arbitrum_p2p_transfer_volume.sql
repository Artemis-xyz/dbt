{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ p2p_transfer_volume("arbitrum") }}