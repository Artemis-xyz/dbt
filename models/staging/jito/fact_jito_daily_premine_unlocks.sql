{{
    config(
        materialized="table",
        snowflake_warehouse="JITO"
    )
}}

select * from {{source('MANUAL_STATIC_TABLES', 'jito_daily_premine_unlocks')}}