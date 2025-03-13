{{
    config(
        materialized="table",
        snowflake_warehouse="CORN",
    )
}}

{{ extract_dune_dex_volumes("corn") }}