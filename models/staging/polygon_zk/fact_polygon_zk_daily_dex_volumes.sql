{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_ZK",
    )
}}

{{ extract_dune_dex_volumes("zkevm") }}