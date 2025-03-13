{{
    config(
        materialized="table",
        snowflake_warehouse="SCROLL",
    )
}}

{{ extract_dune_dex_volumes("scroll") }}