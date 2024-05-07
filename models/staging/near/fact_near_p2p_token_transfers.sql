{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
    )
}}

{{ filter_p2p_token_transfers("near", 1000)}}