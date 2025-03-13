{{
    config(
        materialized="table",
        snowflake_warehouse="INK",
    )
}}

{{ extract_dune_dex_volumes("ink") }}