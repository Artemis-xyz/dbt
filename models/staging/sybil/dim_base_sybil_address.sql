{{ config(materialized="table", snowflake_warehouse="BASE") }}

{{ detect_sybil("base") }}
