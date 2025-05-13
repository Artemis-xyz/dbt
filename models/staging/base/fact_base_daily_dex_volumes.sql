{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
    )
}}

{{ extract_dune_dex_volumes("base") }}