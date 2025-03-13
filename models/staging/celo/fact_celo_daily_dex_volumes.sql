{{
    config(
        materialized="table",
        snowflake_warehouse="CELO",
    )
}}

{{ extract_dune_dex_volumes("celo") }}