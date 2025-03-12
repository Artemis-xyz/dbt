{{
    config(
        materialized="table",
        snowflake_warehouse="FLARE",
    )
}}

{{ extract_dune_dex_volumes("flare") }}