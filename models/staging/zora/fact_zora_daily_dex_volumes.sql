{{
    config(
        materialized="table",
        snowflake_warehouse="ZORA",
    )
}}

{{ extract_dune_dex_volumes("zora") }}