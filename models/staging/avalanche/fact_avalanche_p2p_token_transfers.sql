{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ filter_p2p_token_transfers("avalanche", 1000) }}