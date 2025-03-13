{{
    config(
        materialized="table",
        snowflake_warehouse="BLAST",
    )
}}

{{ extract_dune_dex_volumes("blast") }}