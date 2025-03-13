{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSIS",
    )
}}

{{ extract_dune_dex_volumes("gnosis") }}