{{ config(materialized="incremental", unique_key="trace_id", snowflake_warehouse="CELO_LG") }}

{{ clean_goldsky_traces("celo") }}
