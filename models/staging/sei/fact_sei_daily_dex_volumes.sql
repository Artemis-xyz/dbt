{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}

{{ extract_dune_dex_volumes("sei") }}