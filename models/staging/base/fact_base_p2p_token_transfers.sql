{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
    )
}}

{{ filter_p2p_token_transfers("base", 1000) }}