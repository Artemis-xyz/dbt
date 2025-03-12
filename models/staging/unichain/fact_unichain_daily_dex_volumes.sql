{{
    config(
        materialized="table",
        snowflake_warehouse="UNICHAIN",
    )
}}

{{ extract_dune_dex_volumes("unichain") }}