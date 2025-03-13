{{
    config(
        materialized="table",
        snowflake_warehouse="LINEA",
    )
}}

{{ extract_dune_dex_volumes("linea") }}