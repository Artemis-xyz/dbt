{{
    config(
        materialized="table",
        snowflake_warehouse="BOBA",
    )
}}

{{ extract_dune_dex_volumes("boba") }}