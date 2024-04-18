{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_XS",
    )
}}

{{ filter_p2p_token_transfers("ethereum", 750, blacklist=('0x2b591e99afe9f32eaa6214f7b7629768c40eeb39')) }}