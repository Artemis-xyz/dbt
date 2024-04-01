{{ config(materialized="table", snowflake_warehouse="OPTIMISM") }}

{{ detect_sybil("optimism") }}
