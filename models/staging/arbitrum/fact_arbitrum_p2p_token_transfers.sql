{{
    config(
        materialized="table",
    )
}}

{{ filter_p2p_token_transfers("arbitrum", blacklist=('0xed3fb761414da74b74f33e5c5a1f78104b188dfc')) }}