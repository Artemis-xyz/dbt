{{ config(materialized="table", snowflake_warehouse="ARBITRUM") }}

{{ detect_sybil("arbitrum") }}
