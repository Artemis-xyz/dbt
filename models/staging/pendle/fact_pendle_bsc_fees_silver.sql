{{
    config(
        materialized = 'incremental'
    )
}}

{{ get_pendle_fees_for_chain('bsc') }}