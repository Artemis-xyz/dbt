{{
    config(
        materialized="table",
        snowflake_warehouse="BERACHAIN",
    )
}}

{{ extract_dune_dex_volumes("berachain") }}