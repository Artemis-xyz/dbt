{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ extract_dune_dex_volumes("avalanche_c") }}