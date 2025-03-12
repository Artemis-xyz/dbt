{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ extract_dune_dex_volumes("ethereum") }}