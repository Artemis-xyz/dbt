{{
    config(
        materialized = 'incremental'
        )
}}

{{ get_pendle_daus_txns_for_chain('bsc') }}