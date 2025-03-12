{{
    config(
        materialized="table",
        snowflake_warehouse="MANTLE",
    )
}}

{{ extract_dune_dex_volumes("mantle") }}