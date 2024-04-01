{{ config(materialized="table", snowflake_warehouse="AVALANCHE") }}

{{ detect_sybil("avalanche") }}
