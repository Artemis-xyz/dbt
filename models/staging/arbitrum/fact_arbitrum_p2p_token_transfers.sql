{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ filter_p2p_token_transfers("arbitrum") }}