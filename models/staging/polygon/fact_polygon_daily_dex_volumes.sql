{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON",
    )
}}

{{ extract_dune_dex_volumes("polygon") }}