{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

{{ extract_dune_dex_volumes("arbitrum") }}