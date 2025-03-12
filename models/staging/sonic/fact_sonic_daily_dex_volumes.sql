{{
    config(
        materialized="table",
        snowflake_warehouse="SONIC",
    )
}}

{{ extract_dune_dex_volumes("sonic") }}