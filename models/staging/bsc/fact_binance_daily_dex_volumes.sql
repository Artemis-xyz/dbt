{{
    config(
        materialized="table",
        snowflake_warehouse="BSC",
    )
}}

{{ extract_dune_dex_volumes("bnb") }}