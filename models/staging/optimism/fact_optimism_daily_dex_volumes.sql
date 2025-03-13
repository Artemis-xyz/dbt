{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ extract_dune_dex_volumes("optimism") }}