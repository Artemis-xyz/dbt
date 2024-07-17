{{
    config(
        materialized="table",
    )
}}

{{ filter_p2p_token_transfers("base") }}