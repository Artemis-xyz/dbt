{{
    config(
        materialized="table",
        snowflake_warehouse="ZKSYNC",
    )
}}

{{ extract_dune_dex_volumes("zksync") }}