{{
    config(
        materialized="table",
        snowflake_warehouse="NOVA",
    )
}}

{{ dune_dex_volumes("nova") }}