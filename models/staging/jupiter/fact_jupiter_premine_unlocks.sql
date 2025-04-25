{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER"
    )
}}

select * from {{source('MANUAL_STATIC_TABLES', 'jupiter_daily_premine_unlocks')}}