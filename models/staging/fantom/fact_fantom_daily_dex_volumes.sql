{{
    config(
        materialized="table",
        snowflake_warehouse="FANTOM",
    )
}}

{{ extract_dune_dex_volumes("fantom") }}