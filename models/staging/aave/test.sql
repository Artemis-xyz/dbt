{{ config(materialized="view", snowflake_warehouse='aave') }}
SELECT CURRENT_WAREHOUSE() AS wh, CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch
