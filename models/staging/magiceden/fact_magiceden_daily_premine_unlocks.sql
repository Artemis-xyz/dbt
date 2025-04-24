{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN"
    )
}}

select * from {{source('MANUAL_STATIC_TABLES', 'magiceden_daily_premine_unlocks')}}