{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSIS",
    )
}}

{{ dune_dex_volumes("gnosis") }}