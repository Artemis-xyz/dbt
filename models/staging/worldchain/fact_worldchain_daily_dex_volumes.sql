{{
    config(
        materialized="table",
        snowflake_warehouse="WORLDCHAIN",
    )
}}

{{ extract_dune_dex_volumes("worldchain") }}