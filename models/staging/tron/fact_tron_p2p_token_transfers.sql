{{
    config(
        materialized="table",
        snowflake_warehouse="TRON",
    )
}}

{{ filter_p2p_token_transfers("tron") }}