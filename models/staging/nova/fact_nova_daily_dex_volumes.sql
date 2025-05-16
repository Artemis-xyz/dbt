{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
    )
}}

{{ extract_dune_dex_volumes("nova") }}