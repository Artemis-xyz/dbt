{{
    config(
        materialized="incremental",
        unique_key="block_hash",
        snowflake_warehouse="STORY"
    )
}}
{{ clean_goldsky_blocks("story") }}
