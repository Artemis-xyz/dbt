{{
    config(
        materialized="incremental",
        unique_key="block_hash",
        snowflake_warehouse="CELO_LG"
    )
}}
{{ clean_goldsky_blocks("celo") }}