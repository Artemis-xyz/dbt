{{ config(materialized="table", snowflake_warehouse="ETHEREUM") }}

{{ detect_sybil("ethereum") }}
