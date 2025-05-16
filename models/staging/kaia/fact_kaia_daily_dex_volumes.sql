{{
    config(
        materialized="table",
        snowflake_warehouse="KAIA",
    )
}}

{{ dune_dex_volumes("kaia") }}