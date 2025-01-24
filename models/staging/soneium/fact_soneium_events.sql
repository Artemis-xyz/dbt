{{ config(materialized="table", snowflake_warehouse="SONEIUM") }}


{{ clean_goldsky_events('soneium') }}
