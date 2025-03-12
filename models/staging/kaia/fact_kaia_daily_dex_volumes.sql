{{
    config(
        materialized="table",
        snowflake_warehouse="KAIA",
    )
}}

{{ extract_dune_dex_volumes("kaia") }}