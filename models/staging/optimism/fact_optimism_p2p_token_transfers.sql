{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ filter_p2p_token_transfers("optimism", 1000) }}