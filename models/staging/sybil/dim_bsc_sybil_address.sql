{{ config(materialized="table", snowflake_warehouse="BSC") }} {{ detect_sybil("bsc") }}
