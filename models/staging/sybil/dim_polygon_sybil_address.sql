{{ config(materialized="table", snowflake_warehouse="POLYGON") }}

{{ detect_sybil("polygon") }}
