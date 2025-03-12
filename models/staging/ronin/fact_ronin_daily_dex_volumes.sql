{{
    config(
        materialized="table",
        snowflake_warehouse="RONIN", 
    )
}}

{{ extract_dune_dex_volumes("ronin") }}